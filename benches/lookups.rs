
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use fuzzy_rocks::{*};

pub fn london_lookup_benchmark(c: &mut Criterion) {

    //Initialize the table with a very big database
    let table = Table::<i32, 2, 12, true>::new("all_cities.geonames.rocks").unwrap();

    c.bench_function("london_lookup_exact", |b| b.iter(|| black_box( {
        //let london_results : Vec<i32> = table.lookup_exact("london").unwrap().map(|record_id| table.get_value(record_id).unwrap()).collect();
        let _ = table.lookup_exact("london").unwrap();
    })));

    c.bench_function("london_lookup_best", |b| b.iter(|| black_box( {
        //let london_results : Vec<i32> = table.lookup_exact("london").unwrap().map(|record_id| table.get_value(record_id).unwrap()).collect();
        let _ = table.lookup_best("london", table.default_distance_func()).unwrap();
    })));

}

criterion_group!(benches, london_lookup_benchmark);
criterion_main!(benches);


