package ltsv2avro_test

import (
	"iter"
	"strings"
	"testing"
	"time"

	la "github.com/takanoriyanagitani/go-ltsv2avro"
)

func TestLtsvToAvro(t *testing.T) {
	t.Parallel()

	t.Run("LtsvConfig", func(t *testing.T) {
		t.Parallel()

		t.Run("ToLtsvToMap", func(t *testing.T) {
			t.Parallel()

			t.Run("LtsvConfigDefault", func(t *testing.T) {
				t.Parallel()

				var cfg la.LtsvConfig = la.LtsvConfigDefault
				var l2m la.LtsvToMap = cfg.ToLtsvToMap()

				t.Run("empty", func(t *testing.T) {
					t.Parallel()

					var empty la.LtsvRow
					out := map[string]any{}
					attr := map[string]any{}

					e := l2m(empty, out, attr)
					if nil == e {
						t.Fatalf("must fail\n")
					}
				})

				t.Run("least", func(t *testing.T) {
					t.Parallel()

					var least la.LtsvRow = []la.LtsvItem{
						{Label: "severity", Value: "info"},
						{Label: "body", Value: "hello, world"},
						{
							Label: "timestamp",
							Value: "2025-01-22T14:42:11.012345+09:00",
						},
					}
					out := map[string]any{}
					attr := map[string]any{}

					e := l2m(least, out, attr)
					if nil != e {
						t.Fatalf("unexpected error: %v\n", e)
					}

					if 0 != len(attr) {
						t.Fatalf("must be empty: %v\n", len(attr))
					}

					if 4 != len(out) {
						t.Fatalf("unexpected size: %v\n", len(out))
					}

					_, ok := out["timestamp"].(time.Time)
					if !ok {
						t.Fatalf("unexpected timestamp type\n")
					}

					if string(la.LevelInfo) != out["severity"].(string) {
						t.Fatalf("unexpected severity\n")
					}

					if "hello, world" != out["body"].(string) {
						t.Fatalf("unexpected body\n")
					}
				})

				t.Run("otel", func(t *testing.T) {
					t.Parallel()

					var otel la.LtsvRow = []la.LtsvItem{
						{Label: "severity", Value: "info"},
						{Label: "body", Value: "hello, world"},
						{
							Label: "timestamp",
							Value: "2025-01-22T14:42:11.012345+09:00",
						},
						{Label: "service_name", Value: "user service"},
						{Label: "http_method", Value: "body"},
						{Label: "http_status", Value: "200"},
						{Label: "host_name", Value: "user-svc-main"},
						{Label: "user_name", Value: "postgres"},
						{Label: "pid", Value: "299792458"},
					}
					out := map[string]any{}
					attr := map[string]any{}

					e := l2m(otel, out, attr)
					if nil != e {
						t.Fatalf("unexpected error: %v\n", e)
					}

					if 6 != len(attr) {
						t.Fatalf("unexpected size: %v\n", len(attr))
					}
				})
			})
		})
	})

	t.Run("StringToItems", func(t *testing.T) {
		t.Parallel()

		t.Run("StrToItemsDefault", func(t *testing.T) {
			t.Parallel()

			var s2i la.StringToItems = la.StrToItemsDefault

			t.Run("empty", func(t *testing.T) {
				t.Parallel()

				var inputs iter.Seq2[string, error] = func(
					yield func(string, error) bool,
				) {
				}

				var rows iter.Seq2[la.LtsvRow, error] = s2i.InputsToLtsvRows(
					inputs,
				)
				for range rows {
					t.Fatalf("must be empty\n")
				}
			})

			t.Run("single", func(t *testing.T) {
				t.Parallel()

				var inputs iter.Seq2[string, error] = func(
					yield func(string, error) bool,
				) {
					yield("time:2025-01-22T16:13:42.012345+09:00", nil)
				}

				var rows iter.Seq2[la.LtsvRow, error] = s2i.InputsToLtsvRows(
					inputs,
				)
				for row, e := range rows {
					if nil != e {
						t.Fatalf("unexpected error: %v\n", e)
					}

					if 1 != len(row) {
						t.Fatalf("unexpected len: %v\n", len(row))
					}

					var item la.LtsvItem = row[0]
					var label string = item.Label
					var value string = item.Value

					if label != "time" {
						t.Fatalf("unexpected label: %s\n", label)
					}

					if value != "2025-01-22T16:13:42.012345+09:00" {
						t.Fatalf("unexpected value: %s\n", value)
					}
				}
			})

			t.Run("multi column", func(t *testing.T) {
				t.Parallel()

				var inputs iter.Seq2[string, error] = func(
					yield func(string, error) bool,
				) {
					yield(
						strings.Join([]string{
							"time:2025-01-22T16:17:53.012345+09:00",
							"service_name:user service",
							"http_method:GET",
							"http_status:200",
							"host_name:user-svc-main",
							"user_name:postgres",
							"pid:299792458",
						}, "\t"),
						nil,
					)
				}

				var rows iter.Seq2[la.LtsvRow, error] = s2i.InputsToLtsvRows(
					inputs,
				)
				for row, e := range rows {
					if nil != e {
						t.Fatalf("unexpected error: %v\n", e)
					}

					if 7 != len(row) {
						t.Fatalf("unexpected len: %v\n", len(row))
					}
				}
			})
		})
	})
}
