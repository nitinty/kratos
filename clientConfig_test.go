package kratos

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerateIPFromID_Deterministic(t *testing.T) {
	deviceID := "mac:deadbeefcafe"

	ip1, err := generateIPFromID(deviceID)
	require.NoError(t, err)
	ip2, err := generateIPFromID(deviceID)
	require.NoError(t, err)

	assert.Equal(t, ip1, ip2)
	assert.NotEmpty(t, ip1)
}

func TestGenerateIPFromID_InvalidInput(t *testing.T) {
	_, err := generateIPFromID("mac:short")
	assert.Error(t, err)
}

func TestIntermediateContextJSON(t *testing.T) {
	deviceID := "mac:deadbeefcafe"
	ip, err := generateIPFromID(deviceID)
	require.NoError(t, err)

	intermediateContext := map[string]string{
		"ipAddress":              ip,
		"certificateProviderRaw": "C2",
		"certificateExpiryDate":  "May 16 23:59:59 2031 GMT",
	}
	raw, err := json.Marshal(intermediateContext)
	require.NoError(t, err)

	var decoded map[string]string
	require.NoError(t, json.Unmarshal(raw, &decoded))
	assert.Equal(t, ip, decoded["ipAddress"])
	assert.Equal(t, "C2", decoded["certificateProviderRaw"])
	assert.Equal(t, "May 16 23:59:59 2031 GMT", decoded["certificateExpiryDate"])
}
