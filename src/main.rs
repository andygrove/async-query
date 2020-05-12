use std::pin::Pin;

use tokio::prelude::*;
use tokio::stream::{self, Stream, StreamExt};
use futures::{TryStreamExt};
use futures::stream::BoxStream;

#[tokio::main]
async fn main() {

    let mut results = execute_query().await;


    // let handle = tokio::spawn(async move {
        // show the results
        while let Some(batch) = results.next().await {
            println!("{:?}", batch);
        }
    // });
    //
    // handle.join()
}

async fn execute_query() -> Pin<Box<dyn Stream<Item=ColumnarBatch>>> {

    // create some sample batches of data with two columns
    let iter = stream::iter(vec![create_batch(), create_batch()]);

    // simple projection to swap the column order
    let projection_expr_1: Vec<Box<dyn Expression>> = vec![Box::new(ColumnIndex::new(1)), Box::new(ColumnIndex::new(0))];
    let mut results = create_projection(iter, projection_expr_1);

    // simple projection to swap the column order again
    let projection_expr_2: Vec<Box<dyn Expression>> = vec![Box::new(ColumnIndex::new(1)), Box::new(ColumnIndex::new(0))];
    let mut results = create_projection(results, projection_expr_2);

    results

}

fn create_projection(stream: impl Stream<Item=ColumnarBatch> + 'static + Send, projection_expr: Vec<Box<dyn Expression>>) -> BoxStream<'static, ColumnarBatch> {
    Box::pin(stream.map(move |batch| apply_projection(&batch, &projection_expr)))
}

// Ah right, I forgot you will likely also want the + Send in Pin<Box<dyn Stream<Item=ColumnarBatch>> + Send>> somewhere down the line, or the equivalent but easier to read BoxStream<'static, ColumnarBatch>


//
// async fn now_lets_do_it_with_structs() {
//
//     // create some sample batches of data with two columns
//     let iter = stream::iter(vec![create_batch(), create_batch()]);
//
//     // simple projection to swap the column order
//     let projection_1 = ProjectionExec::new(vec![Box::new(ColumnIndex::new(1)), Box::new(ColumnIndex::new(0))]);
//     let mut results = projection_1.execute(iter);
//
//     // simple projection to swap the column order again
//     let projection_2 = ProjectionExec::new(vec![Box::new(ColumnIndex::new(1)), Box::new(ColumnIndex::new(0))]);
//     let mut results = projection_2.execute(results);
//
//     // show the results
//     while let Some(batch) = results.next().await {
//         println!("{:?}", batch);
//     }
// }

///////////////////////////////////////
// FAILED ATTEMPT AT STRUCT
///////////////////////////////////////


// struct ProjectionExec {
//     expr: Vec<Box<dyn Expression>>,
// }
//
// impl ProjectionExec {
//     pub fn new(expr: Vec<Box<dyn Expression>>) -> Self {
//         Self { expr }
//     }
//
//     fn execute(&self, stream: impl Stream<Item=ColumnarBatch> + 'static) -> Pin<Box<dyn Stream<Item=ColumnarBatch> + '_>> {
//         let projection_expr = self.expr.clone();
//         Box::pin(stream.map(move |batch| apply_projection(&batch, &projection_expr)))
//     }
// }


///////////////////////////////////////
// mock Arrow types below here
///////////////////////////////////////

fn apply_projection(batch: &ColumnarBatch, projection_expr: &Vec<Box<dyn Expression>>) -> ColumnarBatch {
    let columns: Vec<Int32Vector> = projection_expr.iter().map(|expr| expr.evaluate(&batch)).collect();
    ColumnarBatch { columns }
}

#[derive(Clone, Debug)]
struct Int32Vector {
    data: Vec<i32>
}

#[derive(Clone, Debug)]
struct ColumnarBatch {
    columns: Vec<Int32Vector>
}

trait Expression: Send {
    fn evaluate(&self, batch: &ColumnarBatch) -> Int32Vector;
}

struct ColumnIndex {
    i: usize
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
        data: vec![1, 2, 3]
    };
    let b = Int32Vector {
        data: vec![4, 5, 6]
    };
    ColumnarBatch {
        columns: vec![a, b]
    }
}
