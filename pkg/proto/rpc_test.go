package proto

import (
	"strings"
	"testing"
)

func TestHeader_GetSetAddDel(t *testing.T) {
	h := make(Header)
	h.Set("Key", "val1")
	if h.Get("Key") != "val1" {
		t.Fatalf("Get = %q, want val1", h.Get("Key"))
	}
	h.Add("Key", "val2")
	if len(h["Key"]) != 2 {
		t.Fatal("expected 2 values after Add")
	}
	h.Del("Key")
	if h.Get("Key") != "" {
		t.Fatal("expected empty after Del")
	}
	if h.Get("Nonexistent") != "" {
		t.Fatal("expected empty for nonexistent key")
	}
}

func TestMessage_Types(t *testing.T) {
	req := NewRequest("ping", "log1", nil)
	if !req.IsRequest() || req.IsResponse() || req.IsNotification() {
		t.Fatal("expected IsRequest")
	}

	res := NewResponse("log1", 200, nil)
	if !res.IsResponse() || res.IsRequest() || res.IsNotification() {
		t.Fatal("expected IsResponse")
	}

	msg := NewNotification("test", nil)
	if !msg.IsNotification() || msg.IsRequest() || msg.IsResponse() {
		t.Fatal("expected IsNotification")
	}
}

func TestMessage_Error(t *testing.T) {
	m := NewErrorResponse("log1", 500, "something broke")
	if m.Error() != "something broke" {
		t.Fatalf("Error = %q, want 'something broke'", m.Error())
	}

	m2 := NewResponse("log1", 200, nil)
	if m2.Error() != "" {
		t.Fatal("expected empty error")
	}

	m3 := &Message{}
	if m3.Error() != "" {
		t.Fatal("expected empty error for nil header")
	}
}

func TestEncodeDecodeRequest(t *testing.T) {
	body := []byte(`{"key":"value"}`)
	msg := NewRequest("ping", "abc-123", body)

	data := msg.Encode()

	decoded, err := DecodeMessage(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	if decoded.Type != TypeRequest {
		t.Fatalf("Type = %v, want TypeRequest", decoded.Type)
	}
	if decoded.Method != "ping" {
		t.Fatalf("Method = %q, want ping", decoded.Method)
	}
	if decoded.LogId != "abc-123" {
		t.Fatalf("LogId = %q, want abc-123", decoded.LogId)
	}
	if string(decoded.Data) != `{"key":"value"}` {
		t.Fatalf("Data = %q", decoded.Data)
	}
}

func TestEncodeDecodeResponse(t *testing.T) {
	body := []byte(`{"ok":true}`)
	msg := NewResponse("abc-123", 200, body)

	data := msg.Encode()

	decoded, err := DecodeMessage(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	if decoded.Type != TypeResponse {
		t.Fatalf("Type = %v, want TypeResponse", decoded.Type)
	}
	if decoded.LogId != "abc-123" || decoded.Status != 200 {
		t.Fatalf("LogId=%q Status=%d", decoded.LogId, decoded.Status)
	}
}

func TestEncodeDecodeNotification(t *testing.T) {
	body := []byte(`{"event":"joined"}`)
	msg := NewNotification("participant.joined", body)

	data := msg.Encode()

	decoded, err := DecodeMessage(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	if decoded.Type != TypeNotification || decoded.Method != "participant.joined" {
		t.Fatalf("unexpected: Type=%v Method=%q", decoded.Type, decoded.Method)
	}
	if decoded.LogId != "" {
		t.Fatalf("expected empty LogId for notification, got %q", decoded.LogId)
	}
}

func TestEncodeDecodeNotificationWithLogId(t *testing.T) {
	msg := &Message{
		Type:   TypeNotification,
		Method: "test.event",
		LogId:  "trace-1",
		Header: make(Header),
		Data:   []byte("payload"),
	}

	data := msg.Encode()

	decoded, err := DecodeMessage(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if decoded.LogId != "trace-1" {
		t.Fatalf("LogId = %q, want trace-1", decoded.LogId)
	}
}

func TestEncodeDecodeWithHeaders(t *testing.T) {
	msg := NewRequest("test", "log1", []byte("body"))
	msg.Header.Set("Encoding", "gzip")
	msg.Header.Set("Custom", "value")

	data := msg.Encode()

	decoded, err := DecodeMessage(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	if decoded.Header.Get("Encoding") != "gzip" {
		t.Fatalf("Encoding = %q", decoded.Header.Get("Encoding"))
	}
	if decoded.Header.Get("Custom") != "value" {
		t.Fatalf("Custom = %q", decoded.Header.Get("Custom"))
	}
	if string(decoded.Data) != "body" {
		t.Fatalf("Data = %q", decoded.Data)
	}
}

func TestDecodeMessage_Errors(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"no crlf", "REQ ping log1"},
		{"unknown type", "XXX ping log1\r\n"},
		{"req missing parts", "REQ ping\r\n"},
		{"res missing parts", "RES log1\r\n"},
		{"res bad status", "RES log1 abc\r\n"},
		{"msg no method", "MSG\r\n"},
		{"msg too many parts", "MSG a b c d\r\n"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeMessage([]byte(tc.input))
			if err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestEncodeDecodeErrorResponse(t *testing.T) {
	msg := NewErrorResponse("log1", 500, "server error")

	data := msg.Encode()

	decoded, err := DecodeMessage(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	if decoded.Status != 500 || decoded.Error() != "server error" {
		t.Fatalf("Status=%d Error=%q", decoded.Status, decoded.Error())
	}
}

func TestNewJSONRequest(t *testing.T) {
	msg, err := NewJSONRequest("test", "log1", map[string]string{"key": "val"})
	if err != nil {
		t.Fatalf("NewJSONRequest: %v", err)
	}
	if !strings.Contains(string(msg.Data), "key") {
		t.Fatal("expected JSON body")
	}
}

func TestNewJSONResponse(t *testing.T) {
	msg, err := NewJSONResponse("log1", 200, map[string]string{"ok": "true"})
	if err != nil {
		t.Fatalf("NewJSONResponse: %v", err)
	}
	if msg.Status != 200 {
		t.Fatal("expected status 200")
	}
}

func TestNewJSONNotification(t *testing.T) {
	msg, err := NewJSONNotification("event", nil)
	if err != nil {
		t.Fatalf("NewJSONNotification: %v", err)
	}
	if msg.Method != "event" {
		t.Fatal("expected method 'event'")
	}
}

func TestMarshalBody_Nil(t *testing.T) {
	// nil body via NewJSONNotification produces message with nil Data
	msg, err := NewJSONNotification("event", nil)
	if err != nil {
		t.Fatalf("NewJSONNotification(nil): %v", err)
	}
	if msg.Data != nil {
		t.Fatalf("expected nil Data for nil body, got %q", msg.Data)
	}
}
