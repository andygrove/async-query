use std::collections::HashMap;
use std::fs::File;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::thread;

use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::{RecordBatch, RecordBatchReader};

use parquet::arrow::arrow_reader::ArrowReader;
use parquet::arrow::ParquetFileArrowReader;
use parquet::file::reader::SerializedFileReader;

use crossbeam::channel::{unbounded, Receiver, Sender};

use futures::stream::BoxStream;
use futures::task::{Context, Poll};

use tokio::stream::{Stream, StreamExt};
use datafusion::execution::context::ExecutionContext;
use datafusion::logicalplan::{Expr, LogicalPlan, col, aggregate_expr};
use datafusion::error::Result;
use datafusion::error::ExecutionError;
use datafusion::execution::physical_plan::{PhysicalExpr, AggregateExpr};
use datafusion::execution::physical_plan::expressions::{Column, Max};


#[tokio::main]
async fn main() -> Result<()> {

    let mut ctx = ExecutionContext::new();
    ctx.register_parquet("tripdata", "/mnt/nyctaxi/parquet/year=2019/month=05/yellow_tripdata_2019-05.parquet/part-00000-b3313547-ca60-402f-9659-c13bfba3ca8d-c000.snappy.parquet")?;

    let plan = ctx.table("tripdata")?
        .aggregate(vec![col("passenger_count")], vec![aggregate_expr("max", col("fare_amount"), DataType::Float64)])?
        .to_logical_plan();

    let plan = ctx.optimize(&plan)?;
    println!("{:?}", plan);

    //TODO optimize plan

    println!("Compiling query ...");
    let mut stream = compile_query(&mut ctx, &plan)?;

    println!("Fetching results ...");
    while let Some(batch) = stream.next().await {
        println!("Got batch with {} rows", batch.num_rows());
    }

    Ok(())

    // let filenames = vec!["file1.parquet", "file2.parquet"];
    // let handles: Vec<_> = filenames.iter().map(|filename| {
    //     let filename = filename.to_owned();
    //     tokio::spawn(async move {
    //         let partition = mock_read_file(filename);
    //         let mut results = create_query(partition);
    //         print_results(&mut results).await;
    //     })
    // }).collect(); // iterators are lazy so need to collect
    //
    // for handle in handles {
    //     handle.await.unwrap();
    // }
}

fn compile_query(ctx: &mut ExecutionContext, plan: &LogicalPlan) -> Result<BoxStream<'static, RecordBatch>> {
    match plan {
        LogicalPlan::Projection { expr, input, .. } => {
            let input = compile_query(ctx, input.as_ref())?;
            let expr = expr.iter().map(|e| compile_expression(e)).collect::<Result<Vec<_>>>()?;
            create_projection(input, expr)
        },
        LogicalPlan::TableScan { table_name, table_schema, projection, projected_schema, .. } => {
            let table = ctx.table(table_name)?;
            //TODO
            let path = "/mnt/nyctaxi/parquet/year=2019/month=05/yellow_tripdata_2019-05.parquet/part-00000-b3313547-ca60-402f-9659-c13bfba3ca8d-c000.snappy.parquet";
            Ok(Box::pin(ParquetReader::try_new(path, projection.to_owned())?))
        }
        LogicalPlan::Aggregate { input, group_expr, aggr_expr, schema } => {
            let input = compile_query(ctx, input.as_ref())?;
            let group_expr = group_expr.iter().map(|e| compile_expression(e)).collect::<Result<Vec<_>>>()?;
            let aggr_expr = aggr_expr.iter().map(|e| compile_agg_expression(e)).collect::<Result<Vec<_>>>()?;

            //TODO
            unimplemented!()
        }
        _ => unimplemented!()
    }
}

fn compile_expression(expr: &Expr) -> Result<Arc<dyn PhysicalExpr>> {
    match expr {
        Expr::Column(i) => Ok(Arc::new(Column::new(*i, "MAX"))),
        _ => unimplemented!()
    }
}

fn compile_agg_expression(expr: &Expr) -> Result<Arc<dyn AggregateExpr>> {
    match expr {
        Expr::AggregateFunction { name, args, return_type } => {
            //assume MAX for now
            Ok(Arc::new(Max::new(compile_expression(&args[0])?)))
        },
        _ => unimplemented!()
    }
}

fn create_projection(
    stream: impl Stream<Item = RecordBatch> + Send + 'static,
    projection_expr: Vec<Arc<dyn PhysicalExpr>>,
) -> Result<BoxStream<'static, RecordBatch>> {
    Ok(Box::pin(stream.map(move |batch| apply_projection(&batch, &projection_expr))))
}

fn apply_projection(
    batch: &RecordBatch,
    projection_expr: &Vec<Arc<dyn PhysicalExpr>>,
) -> RecordBatch {

    let columns: Vec<ArrayRef> = projection_expr
        .iter()
        .map(|expr| expr.evaluate(&batch))
        .collect::<Result<Vec<_>>>().unwrap();
    RecordBatch::try_new(batch.schema().clone(),  columns).unwrap()
}

struct ParquetReader {
    // schema: Arc<Schema>,
    request_tx: Sender<()>,
    response_rx: Receiver<Result<Option<RecordBatch>>>,
}

impl ParquetReader {

    pub fn try_new(filename: &str, projection: Option<Vec<usize>>) -> Result<Self> {
        let file = File::open(filename)?;
        let file_reader = Rc::new(SerializedFileReader::new(file).unwrap()); //TODO error handling
        let mut arrow_reader = ParquetFileArrowReader::new(file_reader);
        let schema = arrow_reader.get_schema().unwrap(); //TODO error handling

        let projection = match projection {
            Some(p) => p,
            None => (0..schema.fields().len()).collect(),
        };

        let _projected_schema = Schema::new(
            projection
                .iter()
                .map(|i| schema.field(*i).clone())
                .collect(),
        );

        // because the parquet implementation is not thread-safe, it is necessary to execute
        // on a thread and communicate with channels
        let (request_tx, request_rx): (Sender<()>, Receiver<()>) = unbounded();
        let (response_tx, response_rx): (
            Sender<Result<Option<RecordBatch>>>,
            Receiver<Result<Option<RecordBatch>>>,
        ) = unbounded();

        let filename = filename.to_string();

        thread::spawn(move || {
            //TODO error handling, remove unwraps

            let batch_size = 64 * 1024; //TODO

            // open file
            let file = File::open(&filename).unwrap();
            match SerializedFileReader::new(file) {
                Ok(file_reader) => {
                    let file_reader = Rc::new(file_reader);

                    let mut arrow_reader = ParquetFileArrowReader::new(file_reader);

                    match arrow_reader
                        .get_record_reader_by_columns(projection, batch_size)
                    {
                        Ok(mut batch_reader) => {
                            while let Ok(_) = request_rx.recv() {
                                match batch_reader.next_batch() {
                                    Ok(Some(batch)) => {
                                        response_tx.send(Ok(Some(batch))).unwrap();
                                    }
                                    Ok(None) => {
                                        response_tx.send(Ok(None)).unwrap();
                                        break;
                                    }
                                    Err(e) => {
                                        response_tx
                                            .send(Err(ExecutionError::General(format!(
                                                "{:?}",
                                                e
                                            ))))
                                            .unwrap();
                                        break;
                                    }
                                }
                            }
                        }

                        Err(e) => {
                            response_tx
                                .send(Err(ExecutionError::General(format!("{:?}", e))))
                                .unwrap();
                        }
                    }
                }

                Err(e) => {
                    response_tx
                        .send(Err(ExecutionError::General(format!("{:?}", e))))
                        .unwrap();
                }
            }
        });

        println!("try_new ok");

        Ok(Self{
            // schema: projected_schema,
            request_tx,
            response_rx,
        })
    }
}

impl Stream for ParquetReader {

    type Item = RecordBatch;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>)
                 -> Poll<Option<Self::Item>> {

        println!("poll_next()");

        self.request_tx.send(()).unwrap();

        match self.response_rx.recv().unwrap().unwrap() {
            Some(batch) => {
                println!("ready");
                Poll::Ready(Some(batch))
            },
            _ => {
                println!("pending");
                Poll::Pending
            }
        }
    }
}
