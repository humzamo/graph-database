package config

type Config struct {
	AppVersion  string
	Environment string
	Debug       bool
	Neptune     NeptuneConfig
}

type NeptuneConfig struct {
	ReaderEndpoint string
	WriterEndpoint string
}
