use std::pin::Pin;

use tokio::prelude::*;
use tokio::stream::{self, Stream, StreamExt};

#[tokio::main]
async fn main() {
    working_projection_example().await;
}

async fn working_projection_example() {

    let mut stream = stream::iter(vec![create_batch(), create_batch()]);

    // simple projection to swap the column order
    let projection_expr = vec![Box::new(ColumnIndex::new(1)), Box::new(ColumnIndex::new(0))];

    let mut results = stream.map(|batch| {
        let columns: Vec<Int32Vector> = projection_expr.iter().map(|expr| expr.evaluate(&batch)).collect();
        ColumnarBatch { columns }
    });

    while let Some(batch) = results.next().await {
        println!("{:?}", batch);
    }
}




//TODO use Arrow C data interface?

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

trait Exec {
    fn execute(&self) -> Box<dyn Stream<Item=ColumnarBatch>>;
}

struct Projection {
    expr: Vec<Box<dyn Expression>>,
    input: Box<dyn Stream<Item=ColumnarBatch>>
}

impl Exec for Projection {
    fn execute(&self) -> Box<dyn Stream<Item=ColumnarBatch>> {

        unimplemented!()
        // Box::new(self.input.map(|b| {
        //     let columns: Vec<Int32Vector> = self.expr.iter().map(|expr| expr.evaluate(b)).collect();
        //     ColumnarBatch { columns }
        // }))
    }
}

