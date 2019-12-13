// Copyright 2019 Tamás Gulácsi
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package custom

import (
	"bufio"
	"bytes"
	"encoding/xml"
	"time"

	"reflect"
	"unsafe"

	"github.com/gogo/protobuf/types"
	errors "golang.org/x/xerrors"
)

var _ = xml.Unmarshaler((*DateTime)(nil))
var _ = xml.Marshaler(DateTime{})

type DateTime struct {
	time.Time
}

func getWriter(enc *xml.Encoder) *bufio.Writer {
	rEnc := reflect.ValueOf(enc)
	rP := rEnc.Elem().FieldByName("p").Addr()
	return *(**bufio.Writer)(unsafe.Pointer(rP.Elem().FieldByName("Writer").UnsafeAddr()))
}

func (dt DateTime) MarshalXML(enc *xml.Encoder, start xml.StartElement) error {
	//fmt.Printf("Marshal %v: %v\n", start.Name.Local, dt.Time.Format(time.RFC3339))
	if dt.Time.IsZero() {
		start.Attr = append(start.Attr,
			xml.Attr{Name: xml.Name{Space: "http://www.w3.org/2001/XMLSchema-instance", Local: "nil"}, Value: "true"})

		bw := getWriter(enc)
		bw.Flush()
		old := *bw
		var buf bytes.Buffer
		*bw = *bufio.NewWriter(&buf)
		if err := enc.EncodeElement("", start); err != nil {
			return err
		}
		b := bytes.ReplaceAll(bytes.ReplaceAll(buf.Bytes(),
			[]byte("XMLSchema-instance:"), []byte("xsi:")),
			[]byte("xmlns:XMLSchema-instance="), []byte("xmlns:xsi="))
		*bw = old
		bw.Write(b)
		return bw.Flush()
	}
	return enc.EncodeElement(dt.Time.In(time.Local).Format(time.RFC3339), start)
}
func (dt *DateTime) UnmarshalXML(dec *xml.Decoder, st xml.StartElement) error {
	var s string
	if err := dec.DecodeElement(&s, &st); err != nil {
		return err
	}
	return dt.UnmarshalText([]byte(s))
}

func (dt DateTime) MarshalJSON() ([]byte, error) {
	if dt.Time.IsZero() {
		return []byte(`""`), nil
	}
	return dt.Time.In(time.Local).MarshalJSON()
}
func (dt *DateTime) UnmarshalJSON(data []byte) error {
	// Ignore null, like in the main JSON package.
	data = bytes.TrimSpace(data)
	if len(data) == 0 || bytes.Equal(data, []byte(`""`)) || bytes.Equal(data, []byte("null")) {
		return nil
	}
	return dt.UnmarshalText(data)
}

// MarshalText implements the encoding.TextMarshaler interface.
// The time is formatted in RFC 3339 format, with sub-second precision added if present.
func (dt DateTime) MarshalText() ([]byte, error) {
	if dt.Time.IsZero() {
		return nil, nil
	}
	return dt.Time.In(time.Local).MarshalText()
}

// UnmarshalText implements the encoding.TextUnmarshaler interface.
// The time is expected to be in RFC 3339 format.
func (dt *DateTime) UnmarshalText(data []byte) error {
	data = bytes.Trim(data, " \"")
	n := len(data)
	if n == 0 {
		dt.Time = time.Time{}
		//log.Println("time=")
		return nil
	}
	if n > len(time.RFC3339) {
		n = len(time.RFC3339)
	} else if n < 4 {
		n = 4
	} else if n > 10 && data[10] != time.RFC3339[10] {
		data[10] = time.RFC3339[10]
	}
	var err error
	// Fractional seconds are handled implicitly by Parse.
	dt.Time, err = time.ParseInLocation(time.RFC3339[:n], string(data), time.Local)
	//log.Printf("s=%q time=%v err=%+v", data, dt.Time, err)
	if err != nil {
		return errors.Errorf("%s: %w", string(data), err)
	}
	return nil
}

func (dt DateTime) Timestamp() *types.Timestamp {
	ts, _ := types.TimestampProto(dt.Time)
	return ts
}
func (dt DateTime) MarshalTo(dAtA []byte) (int, error) {
	return dt.Timestamp().MarshalTo(dAtA)
}
func (dt DateTime) Marshal() (dAtA []byte, err error) {
	return dt.Timestamp().Marshal()
}
func (dt DateTime) String() string { return dt.Time.In(time.Local).Format(time.RFC3339) }
func (DateTime) ProtoMessage()     {}
func (dt DateTime) ProtoSize() (n int) {
	return dt.Timestamp().ProtoSize()
}
func (dt *DateTime) Reset()       { dt.Time = time.Time{} }
func (dt DateTime) Size() (n int) { return dt.Timestamp().Size() }
func (dt *DateTime) Unmarshal(dAtA []byte) error {
	var ts types.Timestamp
	err := ts.Unmarshal(dAtA)
	if err != nil {
		dt.Time = time.Time{}
		return err
	}
	dt.Time, err = types.TimestampFromProto(&ts)
	return err
}
