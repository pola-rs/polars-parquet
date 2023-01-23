use super::thrift_defined::rosetta::*;
use super::thrift_defined::*;
use crate::errors::{ParquetError, ParquetResult};
use std::rc::Rc;
use std::sync::Arc;

/// Basic type info. This contains information such as the name of the type,
/// the repetition level, the logical type and the kind of the type (group, primitive).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TypeInfo {
    pub name: String,
    pub repetition: Option<Repetition>,
    pub converted_type: Option<ConvertedType>,
    pub logical_type: Option<LogicalType>,
    pub id: Option<i32>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ParquetType {
    Primitive {
        info: TypeInfo,
        physical: PhysicalType,
        type_length: i32,
        scale: i32,
        precision: i32,
    },
    Group {
        info: TypeInfo,
        fields: Vec<ParquetType>,
    },
}

impl ParquetType {
    fn is_group(&self) -> bool {
        matches!(self, Self::Group { .. })
    }
    fn is_primitive(&self) -> bool {
        matches!(self, Self::Primitive { .. })
    }
    fn leaves(&self) -> usize {
        match self {
            Self::Group { fields, .. } => fields.len(),
            Self::Primitive { .. } => 0,
        }
    }
    pub fn fields(&self) -> &[ParquetType] {
        match self {
            Self::Group { fields, .. } => fields,
            _ => panic!("Cannot call fields on a non-group type"),
        }
    }
    fn info(&self) -> &TypeInfo {
        match self {
            Self::Primitive { info, .. } => info,
            Self::Group { info, .. } => info,
        }
    }
    fn physical_type(&self) -> PhysicalType {
        match self {
            Self::Primitive { physical, .. } => *physical,
            _ => panic!("Expected primitive type"),
        }
    }
}

/// Represents a path in a nested schema
#[derive(Clone, PartialEq, Debug, Eq, Hash)]
pub struct ColumnPath {
    pub(crate) parts: Vec<String>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ColumnDescriptor {
    // The "leaf" primitive type of this column
    primitive_type: ParquetType,
    // The maximum definition level for this column
    max_def_level: i16,
    // The maximum repetition level for this column
    max_rep_level: i16,
    // The path of this column. For instance, "a.b.c.d".
    path: ColumnPath,
}
pub type ColumnDescriptorPtr = Rc<ColumnDescriptor>;

impl ColumnDescriptor {
    pub fn converted_type(&self) -> Option<&ConvertedType> {
        self.primitive_type.info().converted_type.as_ref()
    }

    /// Returns [`LogicalType`](crate::basic::LogicalType) for this column.
    pub fn logical_type(&self) -> Option<&LogicalType> {
        self.primitive_type.info().logical_type.as_ref()
    }

    /// Returns physical type for this column.
    /// Note that it will panic if called on a non-primitive type.
    pub fn physical_type(&self) -> PhysicalType {
        match self.primitive_type {
            ParquetType::Primitive { physical, .. } => physical,
            _ => unreachable!(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SchemaDescriptor {
    // The top level schema
    /// This must be a [`ParquetType::Group`] where each field is field is a root column
    /// type in the schema.
    pub(crate) schema: ParquetType,
    pub(crate) leaves: Vec<Rc<ColumnDescriptor>>,
    // Mapping from a leaf column's index to the root column index that it
    // comes from. For instance: the leaf `a.b.c.d` would have a link back to `a`:
    // -- a  <-----+
    // -- -- b     |
    // -- -- -- c  |
    // -- -- -- -- d
    pub(crate) leaf_to_base: Vec<usize>,
}
pub type SchemaDescriptorPtr = Rc<SchemaDescriptor>;

impl SchemaDescriptor {
    pub(crate) fn new(schema_root: ParquetType) -> SchemaDescriptor {
        debug_assert!(schema_root.is_group());
        // lower bound allocation
        let mut leaves = Vec::with_capacity(schema_root.leaves());
        let mut leaf_to_base = vec![];
        for (root_idx, field) in schema_root.fields().iter().enumerate() {
            build_tree(
                field,
                root_idx,
                0,
                0,
                &mut leaves,
                &mut leaf_to_base,
                &mut vec![],
            )
        }
        SchemaDescriptor {
            schema: schema_root,
            leaves,
            leaf_to_base,
        }
    }
}

fn build_tree<'a>(
    tp: &'a ParquetType,
    root_idx: usize,
    mut max_rep_level: i16,
    mut max_def_level: i16,
    leaves: &mut Vec<ColumnDescriptorPtr>,
    leaf_to_base: &mut Vec<usize>,
    path_so_far: &mut Vec<&'a str>,
) {
    let info = tp.info();
    debug_assert!(info.repetition.is_some());

    path_so_far.push(&info.name);
    match info.repetition.unwrap() {
        Repetition::Optional => max_def_level += 1,
        Repetition::Repeated => {
            max_def_level += 1;
            max_rep_level += 1;
        }
        Repetition::Required => {}
    }
    use ParquetType::*;
    match tp {
        Primitive { .. } => {
            let path = path_so_far
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>();
            leaves.push(Rc::new(ColumnDescriptor {
                primitive_type: tp.clone(),
                max_def_level,
                max_rep_level,
                path: ColumnPath { parts: path },
            }))
        }
        Group { fields, .. } => {
            for field in fields {
                build_tree(
                    field,
                    root_idx,
                    max_rep_level,
                    max_def_level,
                    leaves,
                    leaf_to_base,
                    path_so_far,
                );
                path_so_far.pop();
            }
        }
    }
}

pub(crate) fn from_thrift(elements: &[SchemaElement]) -> ParquetResult<ParquetType> {
    let (index, parquet_type) = from_thrift_helper(elements, 0)?;
    if index != elements.len() {
        return Err(ParquetError::InvalidFormat(
            "Expected exactly one root node, but found more.".into(),
        ));
    }
    Ok(parquet_type)
}

fn from_thrift_helper(
    elements: &[SchemaElement],
    index: usize,
) -> ParquetResult<(usize, ParquetType)> {
    // Whether or not the current node is root (message type).
    // There is only one message type node in the schema tree.
    let is_root_node = index == 0;

    let element = &elements[index];
    let converted_type: Option<ConvertedType> = element.converted_type.map(|ct| ct.into());
    // LogicalType is only present in v2 Parquet files. ConvertedType is always
    // populated, regardless of the version of the file (v1 or v2).

    let logical_type: Option<LogicalType> =
        element.logical_type.as_ref().map(|lt| lt.clone().into());
    let mut repetition = element
        .repetition_type
        .map(|r| Repetition::try_from(r))
        .transpose()?;

    let type_info = TypeInfo {
        name: element.name.clone(),
        logical_type,
        converted_type,
        id: element.field_id,
        repetition,
    };
    match element.num_children {
        // From parquet-format:
        //   The children count is used to construct the nested relationship.
        //   This field is not set when the element is a primitive type
        // Sometimes parquet-cpp sets num_children field to 0 for primitive types, so we
        // have to handle this case too.
        None | Some(0) => {
            if repetition.is_none() {
                return Err(ParquetError::InvalidFormat(
                    "Repetition level must be defined for a primitive type".into(),
                ));
            }
            let physical_type = PhysicalType::try_from(element.type_.unwrap())?;
            let type_length = element.type_length.unwrap_or(-1);
            let scale = element.scale.unwrap_or(-1);
            let precision = element.precision.unwrap_or(-1);

            let parquet_type = ParquetType::Primitive {
                info: type_info,
                physical: physical_type,
                type_length,
                scale,
                precision,
            };

            Ok((index + 1, parquet_type))
        }
        Some(n) => {
            let mut current_index = index + 1;
            let children = (0..n)
                .map(|_| {
                    let (index, tp) = from_thrift_helper(elements, current_index)?;
                    current_index = index;
                    Ok(tp)
                })
                .collect::<ParquetResult<Vec<_>>>()?;

            // Sometimes parquet-cpp and parquet-mr set repetition level REQUIRED or
            // REPEATED for root node.
            //
            // We only set repetition for group types that are not top-level message
            // type. According to parquet-format:
            //   Root of the schema does not have a repetition_type.
            //   All other types must have one.
            if is_root_node {
                repetition = None;
            }

            let parquet_type = ParquetType::Group {
                info: type_info,
                fields: children,
            };
            Ok((current_index, parquet_type))
        }
    }
}
