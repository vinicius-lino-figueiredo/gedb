package lib

import (
	"bytes"
	"encoding/json"
	"strconv"
	"time"
)

func marshalAny(buf *bytes.Buffer, value any) error {
	var b []byte
	var err error
	switch v := value.(type) {
	case doc:
		err = v.marshalJSON(buf)
		if err != nil {
			return err
		}
	case list:
		err = v.marshalJSON(buf)
		if err != nil {
			return err
		}
	case string:
		buf.WriteRune('"')
		buf.WriteString(v)
		buf.WriteRune('"')
	case bool:
		buf.WriteString(strconv.FormatBool(v))
	case time.Time:
		buf.WriteString(strconv.Itoa(int(v.Unix())))
	case float64:
		buf.WriteString(strconv.FormatFloat(v, 'f', -1, 64))
	case int64:
		buf.WriteString(strconv.Itoa(int(v)))
	default:
		b, err = json.Marshal(v)
		if err != nil {
			return err
		}
		buf.Write(b)
	}
	return nil
}
