
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use fuzzy_rocks::{*};

pub fn london_lookup_benchmark(c: &mut Criterion) {

    //Initialize the table with a very big database
    let table = Table::<i32, 2, 12, true>::new("all_cities.geonames.rocks").unwrap();

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
        let _iter = table.lookup_best("london", table.default_distance_func()).unwrap();
    })));

    c.bench_function("lookup_best_inexact_london", |b| b.iter(|| black_box( {
        let _iter = table.lookup_best("Rondon", table.default_distance_func()).unwrap();
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

//Unsolved Mysteries: (with Robert Stack)

// GOATGOATGOAT.  lookup_best_inexact_london benchmark is twice as fast as lookup_fuzzy_all_london, but 
//  they're doing the same work.  Get to the bottom of why using counters.
//
// +counter to count the number of variants loaded
//

//GOATGOATGOAT.  It looks like one of the biggest things I can do for performance is to allocate the edit_distance buffer thingy on the stack
