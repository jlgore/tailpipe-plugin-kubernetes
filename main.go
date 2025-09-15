package main

import (
	"log/slog"
	"os"

	"github.com/jlgore/tailpipe-plugin-kubernetes/plugin"
	tailpipeplugin "github.com/turbot/tailpipe-plugin-sdk/plugin"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "metadata" {
		tailpipeplugin.PrintMetadata(plugin.Plugin)
		return
	}

	err := tailpipeplugin.Serve(&tailpipeplugin.ServeOpts{
		PluginFunc: plugin.Plugin,
	})
	if err != nil {
		slog.Error("Error serving plugin", "error", err)
	}
}
