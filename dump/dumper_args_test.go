package dump

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func contains(args []string, want string) bool {
	return slices.Contains(args, want)
}

func newArgsTestDumper() *Dumper {
	d := &Dumper{
		Addr:         "127.0.0.1:3306",
		User:         "root",
		Password:     "secret",
		Charset:      "utf8mb4",
		Tables:       []string{},
		Databases:    []string{},
		IgnoreTables: map[string][]string{},
		ExtraOptions: []string{},
	}
	return d
}

func TestBuildArgs_AllDatabases(t *testing.T) {
	d := newArgsTestDumper()
	args, useDB := d.buildArgs(d.configuredTargets())
	require.Empty(t, useDB)
	require.True(t, contains(args, "--all-databases"))
	require.True(t, contains(args, "--single-transaction"))
	require.True(t, contains(args, "--skip-extended-insert"))
	require.True(t, contains(args, "--password=secret"))
}

func TestBuildArgs_Databases(t *testing.T) {
	d := newArgsTestDumper()
	d.Databases = []string{"db1", "db2"}
	args, useDB := d.buildArgs(d.configuredTargets())
	require.Empty(t, useDB)
	require.True(t, contains(args, "--databases"))
	require.True(t, contains(args, "db1"))
	require.True(t, contains(args, "db2"))
}

func TestBuildArgs_ConfiguredTables(t *testing.T) {
	d := newArgsTestDumper()
	d.TableDB = "shop"
	d.Tables = []string{"orders", "items"}
	d.Where = "id>0"
	d.AddIgnoreTables("shop", "secret")

	args, useDB := d.buildArgs(d.configuredTargets())
	require.Equal(t, "shop", useDB, "caller must emit USE for positional tables")
	require.Equal(t, "shop", args[len(args)-3])
	require.Equal(t, "orders", args[len(args)-2])
	require.Equal(t, "items", args[len(args)-1])
	require.True(t, contains(args, "--where=id>0"))
	require.True(t, contains(args, "--ignore-table=shop.secret"))
}

func TestBuildArgs_SingleTableTarget(t *testing.T) {
	d := newArgsTestDumper()
	args, useDB := d.buildArgs([]dumpTarget{{db: "shop", table: "orders"}})
	require.Equal(t, "shop", useDB)
	require.Len(t, args[len(args)-2:], 2)
	require.Equal(t, "shop", args[len(args)-2])
	require.Equal(t, "orders", args[len(args)-1])
}

func TestBuildArgs_MasterDataAndSourceData(t *testing.T) {
	d := newArgsTestDumper()
	d.TableDB = "shop"
	d.Tables = []string{"orders"}

	d.masterDataSkipped = false
	d.sourceDataSupported = false
	args, _ := d.buildArgs(d.configuredTargets())
	require.True(t, contains(args, "--master-data"))
	require.False(t, contains(args, "--source-data"))

	d.sourceDataSupported = true
	args, _ = d.buildArgs(d.configuredTargets())
	require.True(t, contains(args, "--source-data"))

	d.masterDataSkipped = true
	args, _ = d.buildArgs(d.configuredTargets())
	require.False(t, contains(args, "--master-data"))
	require.False(t, contains(args, "--source-data"))
}

func TestConfiguredTargets(t *testing.T) {
	d := newArgsTestDumper()
	require.Empty(t, d.configuredTargets())

	d.Databases = []string{"db1"}
	require.Equal(t, []dumpTarget{{db: "db1"}}, d.configuredTargets())

	d.TableDB = "shop"
	d.Tables = []string{"a", "b"}
	targets := d.configuredTargets()
	require.Len(t, targets, 2)
	require.Equal(t, "shop", targets[0].db)
	require.Equal(t, "a", targets[0].table)
	require.Equal(t, "b", targets[1].table)
}
