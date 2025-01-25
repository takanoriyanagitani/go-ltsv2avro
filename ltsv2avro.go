package ltsv2avro

import (
	"errors"
	"fmt"
	"iter"
	"maps"
	"strings"
	"time"
)

var (
	ErrInvalidTimestamp error = errors.New("invalid timestamp")
	ErrInvalidBody      error = errors.New("invalid body")

	ErrInvalidLtsv error = errors.New("invalid ltsv")

	ErrInvalidTags error = errors.New("invalid tag type")
)

type Separator string

const SeparatorDefault Separator = "\t"

type LtsvItem struct {
	Label string
	Value string
}

type LtsvRow []LtsvItem

type StringToItems func(input string, output []LtsvItem) ([]LtsvItem, error)

func (c StringToItems) InputsToLtsvRows(
	i iter.Seq2[string, error],
) iter.Seq2[LtsvRow, error] {
	return func(yield func(LtsvRow, error) bool) {
		buf := []LtsvItem{}

		for line, e := range i {
			buf = buf[:0]

			if nil != e {
				yield(nil, e)
				return
			}

			buf, e := c(line, buf)
			if !yield(buf, e) {
				return
			}
		}
	}
}

type StringToStrings func(string) []string

const LabelValueSeparatorDefault string = ":"

func (c StringToStrings) ToStringToItems(sep string) StringToItems {
	return func(i string, o []LtsvItem) ([]LtsvItem, error) {
		var ret []LtsvItem = o[:0]
		var splited []string = c(i)
		for _, raw := range splited {
			// skips an empty string
			if 0 == len(raw) {
				continue
			}

			var pair []string = strings.SplitN(raw, sep, 2)
			if 2 != len(pair) {
				return nil, fmt.Errorf(
					"%w: len=%v, raw=%s, rawlen=%v, i=%s",
					ErrInvalidLtsv,
					len(pair),
					raw,
					len(raw),
					i,
				)
			}

			var label string = pair[0]
			var value string = pair[1]
			ret = append(ret, LtsvItem{
				Label: label,
				Value: value,
			})
		}
		return ret, nil
	}
}

func (s Separator) ToStringToStrings() StringToStrings {
	return func(i string) []string {
		return strings.Split(i, string(s))
	}
}

var StrToStringsDefault StringToStrings = SeparatorDefault.ToStringToStrings()

var StrToItemsDefault StringToItems = StrToStringsDefault.
	ToStringToItems(LabelValueSeparatorDefault)

type Label string

const (
	LabelTimestampDefault Label = "timestamp"

	LabelLevelDefault    Label = "level"
	LabelSeverityDefault Label = "severity"

	LabelMessageDefault Label = "message"
	LabelBodyDefault    Label = "body"

	LabelTagDefault Label = "tag"

	LabelAttributesDefault Label = "attributes"
)

type LabelConfig struct {
	TimestampLabel Label
	SeverityLabel  Label
	BodyLabel      Label

	AttributesLabel Label

	TagLabel Label
}

func (l LabelConfig) ToDeleteFunc() func(string, any) bool {
	return func(key string, val any) bool {
		switch key {
		case string(l.TimestampLabel):
			return true
		case string(l.SeverityLabel):
			return true
		case string(l.BodyLabel):
			return true
		case string(l.TagLabel):
			return true
		default:
			return false
		}
	}
}

func (l LabelConfig) AddTag(tag string, m map[string]any) error {
	var tagKey string = string(l.TagLabel) + "s"
	var prev any = m[tagKey]

	var tags []string
	switch typ := prev.(type) {
	case nil:
		tags = nil
	case []string:
		tags = typ
	default:
		return ErrInvalidTags
	}

	tags = append(tags, tag)
	m[tagKey] = tags

	return nil
}

var LabelConfigDefault LabelConfig = LabelConfig{
	TimestampLabel: LabelTimestampDefault,
	SeverityLabel:  LabelSeverityDefault,
	BodyLabel:      LabelBodyDefault,

	AttributesLabel: LabelAttributesDefault,

	TagLabel: LabelTagDefault,
}

type Timestamp time.Time

type StringToTimestamp func(string) (Timestamp, error)

func (c StringToTimestamp) Or(alt StringToTimestamp) StringToTimestamp {
	return func(s string) (Timestamp, error) {
		parsed, e := c(s)
		switch e {
		case nil:
			return parsed, nil
		default:
			return alt(s)
		}
	}
}

func (c StringToTimestamp) AnyToTimestamp(a any) (Timestamp, error) {
	var empty Timestamp
	switch typ := a.(type) {
	case string:
		return c(typ)
	default:
		return empty, fmt.Errorf("%w: %v", ErrInvalidTimestamp, typ)
	}
}

type TimestampFormat string

const (
	TimestampFormatRFC3339Nano TimestampFormat = time.RFC3339Nano
	TimestampFormatDateTime    TimestampFormat = time.DateTime
)

func (t TimestampFormat) ToStringToTimestamp() StringToTimestamp {
	return func(s string) (Timestamp, error) {
		parsed, e := time.Parse(string(t), s)
		return Timestamp(parsed), e
	}
}

var StrToTimeDefault StringToTimestamp = TimestampFormatRFC3339Nano.
	ToStringToTimestamp().
	Or(TimestampFormatDateTime.ToStringToTimestamp())

type Level string

const (
	LevelUnspecified Level = "LEVEL_UNSPECIFIED"
	LevelTrace       Level = "LEVEL_TRACE"
	LevelDebug       Level = "LEVEL_DEBUG"
	LevelInfo        Level = "LEVEL_INFO"
	LevelWarn        Level = "LEVEL_WARN"
	LevelError       Level = "LEVEL_ERROR"
	LevelFatal       Level = "LEVEL_FATAL"
)

type StringToLevelMap map[string]Level

var StrToLevelMap StringToLevelMap = maps.Collect(
	func(yield func(string, Level) bool,
	) {
		yield("TRACE", LevelTrace)
		yield("DEBUG", LevelDebug)
		yield("INFO", LevelInfo)
		yield("WARN", LevelWarn)
		yield("ERROR", LevelError)
		yield("FATAL", LevelFatal)
	})

type StringToLevel func(string) Level

func (c StringToLevel) ToUpper() StringToLevel {
	return func(s string) Level {
		var upper string = strings.ToUpper(s)
		return c(upper)
	}
}

func (c StringToLevel) AnyToLevel(a any) Level {
	switch typ := a.(type) {
	case string:
		return c(typ)
	default:
		return LevelUnspecified
	}
}

func (m StringToLevelMap) ToStringToLevel() StringToLevel {
	return func(s string) Level {
		mapd, found := m[s]
		switch found {
		case true:
			return mapd
		default:
			return LevelUnspecified
		}
	}
}

var StrToLevelDefault StringToLevel = StrToLevelMap.
	ToStringToLevel().
	ToUpper()

const BlobSizeMaxDefault int = 1048576

type DecodeConfig struct{ BlobSizeMax int }

var DecodeConfigDefault DecodeConfig = DecodeConfig{
	BlobSizeMax: BlobSizeMaxDefault,
}

type Codec string

const (
	CodecNull    Codec = "null"
	CodecDeflate Codec = "deflate"
	CodecSnappy  Codec = "snappy"
	CodecZstd    Codec = "zstandard"
	CodecBzip2   Codec = "bzip2"
	CodecXz      Codec = "xz"
)

const BlockLengthDefault int = 100

type EncodeConfig struct {
	BlockLength int
	Codec
}

var EncodeConfigDefault EncodeConfig = EncodeConfig{
	BlockLength: BlockLengthDefault,
	Codec:       CodecNull,
}

type AvroConfig struct {
	DecodeConfig
	EncodeConfig
}

var AvroConfigDefault AvroConfig = AvroConfig{
	DecodeConfig: DecodeConfigDefault,
	EncodeConfig: EncodeConfigDefault,
}

type LtsvConfig struct {
	LabelConfig
	StringToLevel
	StringToTimestamp
}

func (l LtsvConfig) AnyToBodyString(a any) (string, error) {
	switch typ := a.(type) {
	case string:
		return typ, nil
	default:
		return "", ErrInvalidBody
	}
}

type LtsvToMap func(
	input LtsvRow,
	output map[string]any,
	attr map[string]any,
) error

func (c LtsvToMap) RowsToMaps(
	ltsv iter.Seq2[LtsvRow, error],
) iter.Seq2[map[string]any, error] {
	return func(yield func(map[string]any, error) bool) {
		buf := map[string]any{}
		attr := map[string]any{}

		for row, e := range ltsv {
			clear(buf)
			clear(attr)

			if nil != e {
				yield(buf, e)
				return
			}

			var ce error = c(row, buf, attr)
			if !yield(buf, ce) {
				return
			}
		}
	}
}

func (l LtsvConfig) ToLtsvToMap() LtsvToMap {
	var delFn func(string, any) bool = l.LabelConfig.ToDeleteFunc()

	var itag string = string(l.LabelConfig.TagLabel)

	return func(
		input LtsvRow, out map[string]any, attr map[string]any,
	) error {
		for _, item := range input {
			var label string = item.Label
			var value string = item.Value

			if itag == label {
				e := l.LabelConfig.AddTag(value, out)
				if nil != e {
					return e
				}
				continue
			}

			attr[label] = value
		}

		var timestamp any = attr[string(l.LabelConfig.TimestampLabel)]
		tim, e := l.StringToTimestamp.AnyToTimestamp(timestamp)
		if nil != e {
			return e
		}
		out[string(l.LabelConfig.TimestampLabel)] = time.Time(tim)

		var level any = attr[string(l.LabelConfig.SeverityLabel)]
		var lvl Level = l.StringToLevel.AnyToLevel(level)
		out[string(l.LabelConfig.SeverityLabel)] = string(lvl)

		var body any = attr[string(l.LabelConfig.BodyLabel)]
		bdy, e := l.AnyToBodyString(body)
		if nil != e {
			return e
		}
		out[string(l.LabelConfig.BodyLabel)] = bdy

		maps.DeleteFunc(attr, delFn)

		out[string(l.LabelConfig.AttributesLabel)] = attr

		return nil
	}
}

var LtsvConfigDefault LtsvConfig = LtsvConfig{
	LabelConfig:       LabelConfigDefault,
	StringToLevel:     StrToLevelDefault,
	StringToTimestamp: StrToTimeDefault,
}

type Config struct {
	AvroConfig
	LtsvConfig
}

var ConfigDefault Config = Config{
	AvroConfig: AvroConfigDefault,
	LtsvConfig: LtsvConfigDefault,
}
