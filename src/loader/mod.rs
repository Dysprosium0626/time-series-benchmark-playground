use greptime_proto::v1::{value::ValueData, ColumnDataType, ColumnSchema, SemanticType, Value};

pub mod data_loader;
pub mod greptime_data_loader;

#[inline]
pub fn timestamp_microsecond_value(v: i64) -> Value {
    Value {
        value_data: Some(ValueData::TimestampMicrosecondValue(v)),
    }
}

#[inline]
pub fn string_value(v: String) -> Value {
    Value {
        value_data: Some(ValueData::StringValue(v)),
    }
}

#[inline]
pub fn f32_value(v: f32) -> Value {
    Value {
        value_data: Some(ValueData::F32Value(v)),
    }
}

#[inline]
pub fn i32_value(v: i32) -> Value {
    Value {
        value_data: Some(ValueData::I32Value(v)),
    }
}

pub fn tag(name: &str, datatype: ColumnDataType) -> ColumnSchema {
    ColumnSchema {
        column_name: name.to_string(),
        semantic_type: SemanticType::Tag as i32,
        datatype: datatype as i32,
        ..Default::default()
    }
}

pub fn timestamp(name: &str, datatype: ColumnDataType) -> ColumnSchema {
    ColumnSchema {
        column_name: name.to_string(),
        semantic_type: SemanticType::Timestamp as i32,
        datatype: datatype as i32,
        ..Default::default()
    }
}

pub fn field(name: &str, datatype: ColumnDataType) -> ColumnSchema {
    ColumnSchema {
        column_name: name.to_string(),
        semantic_type: SemanticType::Field as i32,
        datatype: datatype as i32,
        ..Default::default()
    }
}
