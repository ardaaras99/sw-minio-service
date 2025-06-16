import enum

from sw_onto_generation import ENUM_CLASSES

__version__ = "0.1.0"
possible_document_types = [f"{member}" for enum_class in ENUM_CLASSES for member in enum_class._member_names_]
DocumentTypeEnum = enum.StrEnum("DocumentType", possible_document_types)
