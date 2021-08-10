
use rand::prelude::*;
use rand_pcg::Pcg64;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use fuzzy_rocks::{*};

pub fn lookup_benchmark(c: &mut Criterion) {

    //Initialize the table with a very big database
    let config = TableConfig::<char, u8, i32, true>::default();
    let table = Table::new("all_cities.geonames.rocks", config).unwrap();

    c.bench_function("lookup_exact_london", |b| b.iter(|| black_box( {
        let iter = table.lookup_exact("london").unwrap();
        let _ = iter.count();
    })));

    c.bench_function("lookup_exact_tokyo", |b| b.iter(|| black_box( {
        let iter = table.lookup_exact("tokyo").unwrap();
        let _ = iter.count();
    })));

    //This is off the fast-path because the key name is reduced to a MEANINGFUL_KEY_LEN
    c.bench_function("lookup_exact_rio", |b| b.iter(|| black_box( {
        let iter = table.lookup_exact("rio de janeiro").unwrap();
        let _ = iter.count();
    })));

    c.bench_function("lookup_raw_london", |b| b.iter(|| black_box( {
        let iter = table.lookup_fuzzy_raw("london").unwrap();
        let _ = iter.count();
    })));

    c.bench_function("lookup_raw_tokyo", |b| b.iter(|| black_box( {
        let iter = table.lookup_fuzzy_raw("tokyo").unwrap();
        let _ = iter.count();
    })));

    c.bench_function("lookup_best_exact_london", |b| b.iter(|| black_box( {
        let _iter = table.lookup_best("london").unwrap();
    })));

    c.bench_function("lookup_best_inexact_london", |b| b.iter(|| black_box( {
        let _iter = table.lookup_best("Rondon").unwrap();
    })));
    
    //Perform the same fuzzy lookup, but iterate over all of the results
    c.bench_function("lookup_fuzzy_all_london", |b| b.iter(|| black_box( {
        let iter = table.lookup_fuzzy("london", 2).unwrap();
        let _ = iter.count();
    })));

    c.bench_function("lookup_fuzzy_all_tokyo", |b| b.iter(|| black_box( {
        let iter = table.lookup_fuzzy("tokyo", 2).unwrap();
        let _ = iter.count();
    })));

    // This benchmark generates random lowercase ascii + space keys.  Unfortunately (or
    // fortunately depending on your perspective), RocksDB optimizes the DB based on previously
    // queried items.  Therefore, this benchmark gets faster each time you run it.  But
    // that doesn't mean the implementation has actually improved, just that RocksDB is
    // optimizing for the benchmark.
    let mut rng = Pcg64::seed_from_u64(1);
    let mut miss_count = 0;
    let mut hit_count = 0;
    c.bench_function("lookup_fuzzy_all_random", |b| b.iter(|| black_box( {
        let len : usize = rng.gen_range(4..20);
        let mut chars_vec : Vec<u8> = vec![0; len];
        for the_char in chars_vec.iter_mut() {
            *the_char = rng.gen_range(96..123);
            if *the_char == 96 {
                *the_char = ' ' as u8;
            }
        }
        let fuzzy_key : String = chars_vec.into_iter().map(|the_char| the_char as char).collect();
        let iter = table.lookup_fuzzy(&fuzzy_key as &str, 2).unwrap();
        let count = iter.count();
        if count > 0 {
            hit_count += 1;
            // println!("random_fuzzy_key: {}, num_hits: {}", &fuzzy_key, count);
        } else {
            miss_count += 1;
        }
    })));
    // println!("hit_count: {}, miss_count: {}, hit_ratio: {}", hit_count, miss_count, (hit_count as f64) / ((hit_count + miss_count) as f64));

    //TODO: A benchmark that uses actual keys from the DB
}

criterion_group!(benches, lookup_benchmark);
criterion_main!(benches);

//NOTE: invoke flamegraph in criterion with:
// sudo cargo flamegraph --bench lookups -- --bench lookup_fuzzy_all_london

//Solved Mysteries:
//
//* lookup_best is **much** slower compared with lookup_fuzzy.  Like 100x slower.  But both
//should do almost the same amount of work...  What gives??
//ANSWER: It turns out that lookup_best evaluates the distance function for every result, while
// lookup_fuzzy can do that lazily
//
//* "tokyo" has a better lookup_exact speed vs. "london" (~8us vs ~50us), maybe because
//  there are fewer other places also named tokyo, and therefore fewer keys to wade through for
//  the variant I end up loading...
//ANSWER: That hypothesis was true.  The DB had one "Tokyo" record and 8 "London" records.  The
//  speed was killed fetching the record keys.  I added an optimization to not bother fetching
//  the record keys if the meaningful substring didn't shorten the lookup key, which gave a 4x
//  speedup to "tokyo", and a 25x speedup to "london".
//
//* Could we sort the variants by the number of removed characters, with the idea being that we'd
// check the closer matches first, and possibly avoid the need for checking the other matches?
//ANSWER: No.  Sadly that doesn't buy us anything because we can't guarentee the distance function
// correlates to the number of removes.  But we can guarentee that precisely the same pattern will
// have distance zero to itself.  So basically exact matches are a special case, but otherwise we
// need to try all variants.  I added an optimization to call lookup_exact first inside the
// implementation of lookup_best()
//
//* lookup_best_inexact_london benchmark is twice as fast as lookup_fuzzy_all_london, but 
//  they're doing the same work.  Get to the bottom of why using counters.
//ANSWER: They aren't doing the same work.  The key "london" hits twice as many variants and
// consequently many more key groups than the key "Rondon".

//Unsolved Mysteries: (with Robert Stack)
//
// Currently no unsolved mysteries!

