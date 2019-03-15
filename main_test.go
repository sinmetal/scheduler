package main

import (
	"fmt"
	"os"
	"testing"

	_ "github.com/favclip/testerator/datastore"
	_ "github.com/favclip/testerator/memcache"
	_ "github.com/favclip/testerator/search"

	"github.com/favclip/testerator"
)

func TestMain(m *testing.M) {
	_, _, err := testerator.SpinUp() // 最初の1プロセスを起動！

	if err != nil {
		fmt.Printf(err.Error())
		os.Exit(1)
	}

	status := m.Run() // UnitTest実行！

	err = testerator.SpinDown() // 最初に立ち上げたプロセスを落とす
	if err != nil {
		fmt.Printf(err.Error())
		os.Exit(1)
	}

	os.Exit(status)
}
