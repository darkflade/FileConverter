package main

import (
	"FileConverter/config"
	"FileConverter/mediaconverter"
	"FileConverter/routes"
	"fmt"
	"log"

	"github.com/joho/godotenv"
)

func main() {
	fmt.Println("|||||||||||||||FILE CONVERTER SERVER|||||||||||||||||")

	// config Load
	if err := godotenv.Load(); err != nil {
		log.Println(".env not found, using environment variables")
	}
	config.Load()

	mediaconverter.StartWorkers(4)

	// Event transport
	routes.WebTransportRegister()
	select {}
}
