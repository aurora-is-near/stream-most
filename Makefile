# Utilities makefile, only useful for development purposes

NATS_SERVER ?= tls://developer.nats.backend.aurora.dev:4222/
CREDS ?= production_developer.creds
INPUT_QUEUE ?= v3_mainnet_near_blocks

peek_message:
	nats stream view --creds $(CREDS) -s $(NATS_SERVER) --raw $(INPUT_QUEUE) 1
