module github.com/kwilteam/kwil-db/parse

go 1.21.0

replace github.com/kwilteam/kwil-db/core => ../core

require (
	github.com/antlr4-go/antlr/v4 v4.13.0
	github.com/kwilteam/kwil-db/core v0.1.2
	github.com/pganalyze/pg_query_go/v5 v5.1.0
	github.com/stretchr/testify v1.9.0
)

require (
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/google/go-cmp v0.6.0 // indirect
	github.com/google/uuid v1.5.0 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	golang.org/x/exp v0.0.0-20231110203233-9a3e6036ecaa // indirect
	google.golang.org/protobuf v1.31.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
