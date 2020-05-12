use futures::stream::BoxStream;
use tokio::stream::{self, Stream, StreamExt};

#[tokio::main]
async fn main() {
    let filenames = vec!["file1.parquet", "file2.parquet"];
    let handles = filenames.iter().map(|filename| {
        let filename = filename.to_owned();
        tokio::spawn(async move {
            let partition = mock_read_file(filename);
            execute_query(partition).await
        })
    });

    for handle in handles {
        let mut results = handle.await.unwrap();
        print_results(&mut results).await;
    }
}

async fn execute_query(
    iter: impl Stream<Item = ColumnarBatch> + Send + 'static,
) -> BoxStream<'static, ColumnarBatch> {
    // simple projection to swap the column order
    let projection_expr_1: Vec<Box<dyn Expression>> =
        vec![Box::new(ColumnIndex::new(1)), Box::new(ColumnIndex::new(0))];
    let results = create_projection(iter, projection_expr_1);

    // simple projection to swap the column order (again) to demonstrate nested transformations
    let projection_expr_2: Vec<Box<dyn Expression>> =
        vec![Box::new(ColumnIndex::new(1)), Box::new(ColumnIndex::new(0))];
    let results = create_projection(results, projection_expr_2);

    results
}

fn create_projection(
    stream: impl Stream<Item = ColumnarBatch> + Send + 'static,
    projection_expr: Vec<Box<dyn Expression>>,
) -> BoxStream<'static, ColumnarBatch> {
    Box::pin(stream.map(move |batch| apply_projection(&batch, &projection_expr)))
}

/////////////////////////////////////////////////////////////////////////////
// mock Arrow types and helper code below here
/////////////////////////////////////////////////////////////////////////////

fn mock_read_file(_filename: &str) -> BoxStream<'static, ColumnarBatch> {
    //TODO read file for real
    Box::pin(stream::iter(vec![create_batch(), create_batch()]))
}

async fn print_results(results: &mut BoxStream<'static, ColumnarBatch>) {
    while let Some(batch) = results.next().await {
        println!("{:?}", batch);
    }
}

fn apply_projection(
    batch: &ColumnarBatch,
    projection_expr: &Vec<Box<dyn Expression>>,
) -> ColumnarBatch {
    let columns: Vec<Int32Vector> = projection_expr
        .iter()
        .map(|expr| expr.evaluate(&batch))
        .collect();
    ColumnarBatch { columns }
}

#[derive(Clone, Debug)]
struct Int32Vector {
    data: Vec<i32>,
}

#[derive(Clone, Debug)]
struct ColumnarBatch {
    columns: Vec<Int32Vector>,
}

trait Expression: Send {
    fn evaluate(&self, batch: &ColumnarBatch) -> Int32Vector;
}

struct ColumnIndex {
    i: usize,
}

impl ColumnIndex {
    pub fn new(i: usize) -> Self {
        Self { i }
    }
}

impl Expression for ColumnIndex {
    fn evaluate(&self, batch: &ColumnarBatch) -> Int32Vector {
        batch.columns[self.i].clone()
    }
}

fn create_batch() -> ColumnarBatch {
    let a = Int32Vector {
        data: vec![1, 2, 3],
    };
    let b = Int32Vector {
        data: vec![4, 5, 6],
    };
    ColumnarBatch {
        columns: vec![a, b],
    }
}
