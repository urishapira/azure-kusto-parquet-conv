use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::Field;
use parquet::schema::types::Type as SchemaType;
use serde_json::Value;
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::Path;

use crate::settings::Settings;
use csv::Terminator;
use parquet::record::reader::RowIter;

const WRITER_BUF_CAP: usize = 256 * 1024;

/// Writes Parquet file as text, either as JSONL (every line contains single JSON record)
/// or as CSV (where nested structures are formatted as JSON strings).
///
/// Arguments:
///
/// * `settings` - Converter settings
/// * `input_file` - Parquet file path
/// * `output_file` - Optional output file path (if not provided - output is written to STDOUT).
///
pub fn convert(
    settings: &Settings,
    input_file: &str,
    output_file: Option<&str>,
) -> Result<(), Box<dyn Error>> {
    let file = File::open(&Path::new(input_file))?;
    let reader = SerializedFileReader::new(file)?;

    let writer = match output_file {
        Some(output_file) => Box::new(BufWriter::with_capacity(
            WRITER_BUF_CAP,
            File::create(&Path::new(output_file))?,
        )) as Box<dyn Write>,
        None => Box::new(BufWriter::with_capacity(WRITER_BUF_CAP, io::stdout())) as Box<dyn Write>,
    };

    let mut missing_columns = std::collections::HashSet::new();
    let schema = settings
        .columns
        .as_ref()
        .map(|c| projected_schema(&reader, &c, &mut missing_columns).unwrap());

    let rows = reader.get_row_iter(schema)?;

    if settings.csv {
        top_level_rows_to_csv(&settings, rows, missing_columns, writer)
    } else {
        top_level_rows_to_json(rows, writer)
    }
}

fn projected_schema(
    reader: &SerializedFileReader<File>,
    columns: &Vec<String>,
    missing_columns: &mut std::collections::HashSet<std::string::String>,
) -> Result<SchemaType, Box<dyn Error>> {
    let file_meta = reader.metadata().file_metadata();
    let mut schema_fields = HashMap::new();
    for field in file_meta.schema().get_fields().iter() {
        schema_fields.insert(field.name().to_owned(), field);
    }

    let mut projected_fields = Vec::new();

    for c in columns.iter() {
        let res = schema_fields.get_mut(c);

        match res {
            Some(ptr) => {
                projected_fields.push(ptr.clone());
            }
            None => {
                missing_columns.insert(c.clone());
            }
        }
    }

    Ok(
        SchemaType::group_type_builder(&file_meta.schema().get_basic_info().name())
            .with_fields(&mut projected_fields)
            .build()
            .unwrap(),
    )
}

fn top_level_rows_to_json(
    mut rows: RowIter,
    mut writer: Box<dyn Write>,
) -> Result<(), Box<dyn Error>> {
    while let Some(row) = rows.next() {
        let value = row.to_json_value();
        let value = if value.is_null() {
            Value::Object(serde_json::Map::default())
        } else {
            value
        };
        writeln!(writer, "{}", serde_json::to_string(&value)?)?;
    }
    Ok(())
}

fn top_level_rows_to_csv(
    settings: &Settings,
    mut rows: RowIter,
    missing_columns: std::collections::HashSet<std::string::String>,
    mut writer: Box<dyn Write>,
) -> Result<(), Box<dyn Error>> {
    while let Some(row) = rows.next() {
        let mut csv_writer = csv::WriterBuilder::new()
            .terminator(Terminator::Any(b'\r'))
            .from_writer(vec![]);
        let columns = settings.columns.as_ref();
        match columns {
            Some(cols) => {
                let mut row_columns_map: HashMap<&String, &Field> = HashMap::new();
                for (name, field) in row.get_column_iter() {
                    row_columns_map.insert(name, field);
                }

                // Produce empty values for columns specified by --columns argument, but missing in the file
                for col in cols {
                    let value = if missing_columns.contains(col) {
                        Value::Null
                    } else {
                        let field_type = row_columns_map.get(col).expect("Column does not exist.");
                        field_type.to_json_value()
                    };

                    csv_writer.write_field(value_to_csv(&value))?;
                }
            }
            None => {
                // No columns specified by --columns argument
                for (_, field) in row.get_column_iter() {
                    let value = field.to_json_value();
                    csv_writer.write_field(value_to_csv(&value))?;
                }
            }
        };

        csv_writer.write_record(None::<&[u8]>)?;
        writeln!(writer, "{}", String::from_utf8(csv_writer.into_inner()?)?)?;
    }
    Ok(())
}

fn value_to_csv(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Bool(v) => v.to_string(),
        Value::Number(ref v) => {
            if v.is_f64() {
                let mut buffer = ryu::Buffer::new();
                buffer.format(v.as_f64().unwrap()).to_owned()
            } else if v.is_u64() {
                format!("{}", v.as_u64().unwrap())
            } else {
                format!("{}", v.as_i64().unwrap())
            }
        }
        Value::String(ref v) => v.to_owned(),
        Value::Array(ref v) => serde_json::to_string(&v).unwrap(),
        Value::Object(ref v) => serde_json::to_string(&v).unwrap(),
    }
}
