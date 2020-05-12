use std::pin::Pin;

use tokio::prelude::*;
use tokio::stream::{self, Stream, StreamExt};
use futures::{TryStreamExt};

#[tokio::main]
async fn main() {
    working_projection_example().await;
}

async fn working_projection_example() {

    // create some sample batches of data with two columns
    let iter = stream::iter(vec![create_batch(), create_batch()]);

    // simple projection to swap the column order
    let projection_expr_1: Vec<Box<dyn Expression>> = vec![Box::new(ColumnIndex::new(1)), Box::new(ColumnIndex::new(0))];
    let mut results = create_projection(iter, projection_expr_1).await;

    // simple projection to swap the column order again
    let projection_expr_2: Vec<Box<dyn Expression>> = vec![Box::new(ColumnIndex::new(1)), Box::new(ColumnIndex::new(0))];
    let mut results = create_projection(results, projection_expr_2).await;

    // show the results
    while let Some(batch) = results.next().await {
        println!("{:?}", batch);
    }
}

async fn create_projection(stream: impl Stream<Item=ColumnarBatch> + 'static, projection_expr: Vec<Box<dyn Expression>>) -> Pin<Box<dyn Stream<Item=ColumnarBatch>>> {
    Box::pin(stream.map(move |batch| apply_projection(&batch, &projection_expr)))
}

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

trait Expression {
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
