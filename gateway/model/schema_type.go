package model

type (

	// schemaType is the data structure for storing the parsed values of schema string
	SchemaType       map[string]SchemaCollection // key is database name
	SchemaCollection map[string]SchemaFields     // key is collection name
	SchemaFields     map[string]*SchemaFieldType // key is field name
	directiveArgs    map[string]string           // key is Directive's argument name
	fieldType        int

	SchemaFieldType struct {
		FieldName           string
		IsFieldTypeRequired bool
		IsList              bool
		Kind                string
		//Directive           string
		NestedObject SchemaFields

		// For directives
		IsPrimary   bool
		IsIndex     bool
		IsUnique    bool
		IsCreatedAt bool
		IsUpdatedAt bool
		IsLinked    bool
		IsForeign   bool
		IsDefault   bool
		IndexInfo   *TableProperties
		LinkedTable *TableProperties
		JointTable  *TableProperties
		Default     interface{}
	}

	TableProperties struct {
		From, To     string
		Table, Field string
		DBType       string
		Group, Sort  string
		Order        int
	}
)

const (
	TypeInteger        string = "Integer"
	TypeString         string = "String"
	TypeFloat          string = "Float"
	TypeBoolean        string = "Boolean"
	TypeDateTime       string = "DateTime"
	TypeID             string = "ID"
	SqlTypeIDSize      string = "50"
	TypeObject         string = "Object"
	TypeEnum           string = "Enum"
	DirectiveUnique    string = "unique"
	DirectiveIndex     string = "index"
	DirectiveForeign   string = "foreign"
	DirectivePrimary   string = "primary"
	DirectiveCreatedAt string = "createdAt"
	DirectiveUpdatedAt string = "updatedAt"
	DirectiveLink      string = "link"
	DirectiveDefault   string = "default"

	DefaultIndexName  string = ""
	DefaultIndexSort  string = "asc"
	DefaultIndexOrder int    = 1
)
