package enc

import (
	"context"
	"io"
	"iter"
	"log"
	"os"

	ha "github.com/hamba/avro/v2"
	ho "github.com/hamba/avro/v2/ocf"
	la "github.com/takanoriyanagitani/go-ltsv2avro"
	. "github.com/takanoriyanagitani/go-ltsv2avro/util"
)

func MapsToWriterHamba(
	ctx context.Context,
	imap iter.Seq2[map[string]any, error],
	wtr io.Writer,
	s ha.Schema,
	opts ...ho.EncoderFunc,
) error {
	enc, err := ho.NewEncoderWithSchema(
		s,
		wtr,
		opts...,
	)
	if nil != err {
		return err
	}
	defer enc.Close()

	for row, err := range imap {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if nil != err {
			return err
		}

		err = enc.Encode(row)
		if nil != err {
			log.Printf("row: %v\n", row)
			return err
		}

		err = enc.Flush()
		if nil != err {
			return err
		}
	}

	return enc.Flush()
}

func CodecConv(c la.Codec) ho.CodecName {
	switch c {
	case la.CodecNull:
		return ho.Null
	case la.CodecDeflate:
		return ho.Deflate
	case la.CodecSnappy:
		return ho.Snappy
	case la.CodecZstd:
		return ho.ZStandard

	// unsupported
	case la.CodecBzip2:
		return ho.Null
	case la.CodecXz:
		return ho.Null

	default:
		return ho.Null
	}
}

func ConfigToOpts(cfg la.EncodeConfig) []ho.EncoderFunc {
	var c ho.CodecName = CodecConv(cfg.Codec)

	return []ho.EncoderFunc{
		ho.WithBlockLength(cfg.BlockLength),
		ho.WithCodec(c),
	}
}

func MapsToWriter(
	ctx context.Context,
	imap iter.Seq2[map[string]any, error],
	wtr io.Writer,
	schema string,
	cfg la.EncodeConfig,
) error {
	parsed, e := ha.Parse(schema)
	if nil != e {
		return e
	}

	var opts []ho.EncoderFunc = ConfigToOpts(cfg)

	return MapsToWriterHamba(
		ctx,
		imap,
		wtr,
		parsed,
		opts...,
	)
}

func MapsToStdout(
	ctx context.Context,
	imap iter.Seq2[map[string]any, error],
	schema string,
	cfg la.EncodeConfig,
) error {
	return MapsToWriter(
		ctx,
		imap,
		os.Stdout,
		schema,
		cfg,
	)
}

func MapsToStdoutDefault(
	ctx context.Context,
	m iter.Seq2[map[string]any, error],
	schema string,
) error {
	return MapsToStdout(ctx, m, schema, la.EncodeConfigDefault)
}

func SchemaToMapsToStdoutDefault(
	schema string,
) func(iter.Seq2[map[string]any, error]) IO[Void] {
	return func(m iter.Seq2[map[string]any, error]) IO[Void] {
		return func(ctx context.Context) (Void, error) {
			return Empty, MapsToStdoutDefault(
				ctx,
				m,
				schema,
			)
		}
	}
}
