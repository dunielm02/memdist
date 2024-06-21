package db_test

import (
	"testing"

	"github.com/dunielm02/memdist/api/v1"
	"github.com/dunielm02/memdist/internal/db"
	"github.com/stretchr/testify/require"
)

func TestDb(t *testing.T) {
	data := db.New()

	testCases := map[string]string{
		"foo":  "bar",
		"john": "doe",
	}

	for k, v := range testCases {
		data.Set(&api.SetRequest{
			Key:   k,
			Value: v,
		})
	}

	for k, v := range testCases {
		res, err := data.Get(&api.GetRequest{Key: k})
		require.NoError(t, err)
		require.Equal(t, res.Value, v)
	}

	all, err := data.Read()
	require.NoError(t, err)
	require.Equal(t, len(all), len(testCases))

	err = data.Delete(&api.DeleteRequest{Key: "foo"})
	require.NoError(t, err)

	all, err = data.Read()
	require.NoError(t, err)
	require.Equal(t, len(all), len(testCases)-1)

	require.NoError(t, data.Reset())

	all, err = data.Read()
	require.NoError(t, err)
	require.Equal(t, len(all), 0)
}
