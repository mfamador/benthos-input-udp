// main package for main application
package main

import (
	"github.com/Jeffail/benthos/v3/lib/service"
	_ "github.com/mfamador/benthos-input-udp/input"
	"github.com/rs/zerolog/log"
)

func main() {
	log.Info().Msg("Starting UDP server")

	service.Run()
}
