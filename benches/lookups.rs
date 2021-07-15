
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use fuzzy_rocks::{*};

pub fn london_lookup_benchmark(c: &mut Criterion) {

    //Initialize the table with a very big database
    let table = Table::<i32, 2, 12, true>::new("all_cities.geonames.rocks").unwrap();

    c.bench_function("lookup_exact_london", |b| b.iter(|| black_box( {
        let _iter = table.lookup_exact("london").unwrap();
    })));

    c.bench_function("lookup_exact_tokyo", |b| b.iter(|| black_box( {
        let _iter = table.lookup_exact("tokyo").unwrap();
    })));

    c.bench_function("lookup_best_london", |b| b.iter(|| black_box( {
        let _iter = table.lookup_best("london", table.default_distance_func()).unwrap();
    })));

    c.bench_function("lookup_best_tokyo", |b| b.iter(|| black_box( {
        let _iter = table.lookup_best("tokyo", table.default_distance_func()).unwrap();
    })));

    //Perform a fuzzy lookup, but stop after we get the first result 
    c.bench_function("lookup_fuzzy_one_london", |b| b.iter(|| black_box( {
        let mut iter = table.lookup_fuzzy("london", table.default_distance_func(), 3).unwrap();
        let _ = iter.next().unwrap();
    })));

    c.bench_function("lookup_fuzzy_one_tokyo", |b| b.iter(|| black_box( {
        let mut iter = table.lookup_fuzzy("tokyo", table.default_distance_func(), 3).unwrap();
        let _ = iter.next().unwrap();
    })));
    
    //Perform the same fuzzy lookup, but iterate over all of the results
    c.bench_function("lookup_fuzzy_all_london", |b| b.iter(|| black_box( {
        let iter = table.lookup_fuzzy("london", table.default_distance_func(), 3).unwrap();
        let _ = iter.count();
    })));

    c.bench_function("lookup_fuzzy_all_tokyo", |b| b.iter(|| black_box( {
        let iter = table.lookup_fuzzy("tokyo", table.default_distance_func(), 3).unwrap();
        let _ = iter.count();
    })));

}

criterion_group!(benches, london_lookup_benchmark);
criterion_main!(benches);


//Interesting observations:

//* QUESTION: lookup_best is **much** slower compared with lookup_fuzzy.  Like 100x slower.  But both
//should do almost the same amount of work...  What gives??
// ANSWER: It turns out that lookup_best evaluates the distance function for every result, while
// lookup_fuzzy can do that lazily


//* "tokyo" has a better lookup_exact speed vs. "london" (~8us vs ~50us), maybe because
//  there are fewer other places also named tokyo, and therefore fewer keys to wade through for
//  the variant I end up loading...
//
//* "london" has a better lookup_best speed vs. "tokyo" (~5ms vs 8ms), maybe because "tokyo"
//  is a shorter name and therefore has a lot more variants that end up hitting DB entries
//
//Should confirm with perf counters


