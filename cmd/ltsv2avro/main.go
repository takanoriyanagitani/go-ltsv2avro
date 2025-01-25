package main

import (
	"context"
	"fmt"
	"io"
	"iter"
	"log"
	"os"
	"strings"

	la "github.com/takanoriyanagitani/go-ltsv2avro"
	eh "github.com/takanoriyanagitani/go-ltsv2avro/avro/enc/hamba"
	ds "github.com/takanoriyanagitani/go-ltsv2avro/ltsv/dec/std"
	. "github.com/takanoriyanagitani/go-ltsv2avro/util"
)

var EnvValByKey func(string) IO[string] = Lift(
	func(key string) (string, error) {
		val, found := os.LookupEnv(key)
		switch found {
		case true:
			return val, nil
		default:
			return "", fmt.Errorf("env var %s missing", key)
		}
	},
)

var rawStrings IO[iter.Seq2[string, error]] = ds.StdinToRawStrings

var str2items la.StringToItems = la.StrToItemsDefault

var ltsvRows IO[iter.Seq2[la.LtsvRow, error]] = Bind(
	rawStrings,
	Lift(func(
		raw iter.Seq2[string, error],
	) (iter.Seq2[la.LtsvRow, error], error) {
		return str2items.InputsToLtsvRows(raw), nil
	}),
)

var timestampLabel IO[la.Label] = Bind(
	EnvValByKey("ENV_LABEL_TIMESTAMP"),
	Lift(func(s string) (la.Label, error) { return la.Label(s), nil }),
).Or(Of(la.LabelTimestampDefault))

var severityLabel IO[la.Label] = Bind(
	EnvValByKey("ENV_LABEL_SEVERITY"),
	Lift(func(s string) (la.Label, error) { return la.Label(s), nil }),
).Or(Of(la.LabelSeverityDefault))

var bodyLabel IO[la.Label] = Bind(
	EnvValByKey("ENV_LABEL_BODY"),
	Lift(func(s string) (la.Label, error) { return la.Label(s), nil }),
).Or(Of(la.LabelBodyDefault))

var attrLabel IO[la.Label] = Bind(
	EnvValByKey("ENV_LABEL_ATTR"),
	Lift(func(s string) (la.Label, error) { return la.Label(s), nil }),
).Or(Of(la.LabelAttributesDefault))

var tagLabel IO[la.Label] = Bind(
	EnvValByKey("ENV_LABEL_TAG"),
	Lift(func(s string) (la.Label, error) { return la.Label(s), nil }),
).Or(Of(la.LabelTagDefault))

var labelConfig IO[la.LabelConfig] = Bind(
	All(
		timestampLabel,
		severityLabel,
		bodyLabel,
		attrLabel,
		tagLabel,
	),
	Lift(func(s []la.Label) (la.LabelConfig, error) {
		return la.LabelConfig{
			TimestampLabel:  s[0],
			SeverityLabel:   s[1],
			BodyLabel:       s[2],
			AttributesLabel: s[3],
			TagLabel:        s[4],
		}, nil
	}),
)

var ltsvCfg IO[la.LtsvConfig] = Bind(
	labelConfig,
	Lift(func(c la.LabelConfig) (la.LtsvConfig, error) {
		return la.LtsvConfig{
			LabelConfig:       c,
			StringToLevel:     la.StrToLevelDefault,
			StringToTimestamp: la.StrToTimeDefault,
		}, nil
	}),
)

var ltsv2map IO[la.LtsvToMap] = Bind(
	ltsvCfg,
	Lift(func(c la.LtsvConfig) (la.LtsvToMap, error) {
		return c.ToLtsvToMap(), nil
	}),
)

var mapd IO[iter.Seq2[map[string]any, error]] = Bind(
	ltsvRows,
	func(
		rows iter.Seq2[la.LtsvRow, error],
	) IO[iter.Seq2[map[string]any, error]] {
		return Bind(
			ltsv2map,
			Lift(func(
				l2m la.LtsvToMap,
			) (iter.Seq2[map[string]any, error], error) {
				return l2m.RowsToMaps(rows), nil
			}),
		)
	},
)

var schemaFilename IO[string] = EnvValByKey("ENV_SCHEMA_FILENAME")

func FilenameToStringLimited(limit int64) func(string) IO[string] {
	return Lift(func(filename string) (string, error) {
		file, err := os.Open(filename)
		if nil != err {
			return "", fmt.Errorf("%w: filename=%s", err, filename)
		}

		limited := &io.LimitedReader{
			R: file,
			N: limit,
		}

		var buf strings.Builder
		_, err = io.Copy(&buf, limited)

		return buf.String(), err
	})
}

const SchemaFileSizeMaxDefault int64 = 1048576

var schemaContent IO[string] = Bind(
	schemaFilename,
	FilenameToStringLimited(SchemaFileSizeMaxDefault),
)

var stdin2ltsv2mapd2avro2stdout IO[Void] = Bind(
	schemaContent,
	func(s string) IO[Void] {
		return Bind(
			mapd,
			eh.SchemaToMapsToStdoutDefault(s),
		)
	},
)

var sub IO[Void] = func(ctx context.Context) (Void, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	return stdin2ltsv2mapd2avro2stdout(ctx)
}

func main() {
	_, e := sub(context.Background())
	if nil != e {
		log.Printf("%v\n", e)
	}
}
