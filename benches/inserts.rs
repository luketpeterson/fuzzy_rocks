use std::{collections::HashSet, path::PathBuf};

use csv::ReaderBuilder;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use fuzzy_rocks::{unicode_string_helpers::unicode_truncate, *};
use serde::{Deserialize, Serialize};

pub fn lookup_benchmark(c: &mut Criterion) {
    //Configure and Create the FuzzyRocks Table using a very big database
    struct Config();
    impl TableConfig for Config {
        type KeyCharT = char;
        type DistanceT = u8;
        type ValueT = i32;
        type CoderT = DefaultCoder;
    }
    let mut table = Table::<Config, true>::new("bench_insert.geonames.rocks", Config()).unwrap();

    let mut geonames_file_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    geonames_file_path.push("geonames_megacities.txt");

    //Clear out any records that happen to be hanging out
    table.reset().unwrap();

    //Data structure to parse the GeoNames TSV file into
    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct GeoName {
        geonameid: i32,         //integer id of record in geonames database
        name: String,           //name of geographical point (utf8) varchar(200)
        asciiname: String,      //name of geographical point in plain ascii characters, varchar(200)
        alternatenames: String, //alternatenames, comma separated, ascii names automatically transliterated, convenience attribute from alternatename table, varchar(10000)
        latitude: f32,          //latitude in decimal degrees (wgs84)
        longitude: f32,         //longitude in decimal degrees (wgs84)
        feature_class: char,    //see http://www.geonames.org/export/codes.html, char(1)
        feature_code: String, //[char; 10], //see http://www.geonames.org/export/codes.html, varchar(10)
        country_code: String, //[char; 2], //ISO-3166 2-letter country code, 2 characters
        cc2: String, //alternate country codes, comma separated, ISO-3166 2-letter country code, 200 characters
        admin1_code: String, //[char; 20], //fipscode (subject to change to iso code), see exceptions below, see file admin1Codes.txt for display names of this code; varchar(20)
        admin2_code: String, //code for the second administrative division, a county in the US, see file admin2Codes.txt; varchar(80)
        admin3_code: String, //[char; 20], //code for third level administrative division, varchar(20)
        admin4_code: String, //[char; 20], //code for fourth level administrative division, varchar(20)
        population: i64,     //bigint (8 byte int)
        #[serde(deserialize_with = "default_if_empty")]
        elevation: i32, //in meters, integer
        #[serde(deserialize_with = "default_if_empty")]
        dem: i32, //digital elevation model, srtm3 or gtopo30, average elevation of 3''x3'' (ca 90mx90m) or 30''x30'' (ca 900mx900m) area in meters, integer. srtm processed by cgiar/ciat.
        timezone: String, //the iana timezone id (see file timeZone.txt) varchar(40)
        modification_date: String, //date of last modification in yyyy-MM-dd format
    }

    fn default_if_empty<'de, D, T>(de: D) -> Result<T, D::Error>
    where
        D: serde::Deserializer<'de>,
        T: serde::Deserialize<'de> + Default,
    {
        Option::<T>::deserialize(de).map(|x| x.unwrap_or_else(|| T::default()))
    }

    //Open the tab-saparated value file
    let tsv_file_contents =
        std::fs::read_to_string(geonames_file_path).expect("Error reading geonames file");
    let mut tsv_parser = ReaderBuilder::new()
        .delimiter(b'\t')
        .has_headers(false)
        .flexible(true) //We want to permit situations where some rows have fewer columns for now
        .quote(0)
        .double_quote(false)
        .from_reader(tsv_file_contents.as_bytes());

    //Iterate over every geoname entry in the geonames file and insert it (lowercase) into our table
    let geo_names = tsv_parser
        .deserialize::<GeoName>()
        .map(|result| result.unwrap())
        .collect::<Vec<_>>();

    c.bench_function("insert", move |b| {
        b.iter(|| {
            black_box({
                for geoname in &geo_names {
                    //Separate the comma-separated alternatenames field
                    let mut names: HashSet<String> = HashSet::from_iter(
                        geoname
                            .alternatenames
                            .split(',')
                            .map(|string| string.to_lowercase()),
                    );

                    //Add the primary name for the place
                    names.insert(geoname.name.to_lowercase());

                    //Create a record in the table
                    let names_vec: Vec<String> = names
                        .into_iter()
                        .map(|string| unicode_truncate(string.as_str(), MAX_KEY_LENGTH))
                        .collect();
                    table.create(&names_vec[..], &geoname.geonameid).unwrap();
                }
                1
            })
        })
    });
}

criterion_group!(benches, lookup_benchmark);
criterion_main!(benches);
