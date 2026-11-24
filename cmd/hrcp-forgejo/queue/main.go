// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package main

import (
	"context"
	"encoding/json/v2"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"os/signal"
	"syscall"

	"code.forgejo.org/forgejo/runner/v13/hrcbridge"
	"dimidiumlabs.io/hrc/internal/config"
	"dimidiumlabs.io/hrc/internal/provider"
	"github.com/google/uuid"
)

type forgeConfig struct {
	config.Provider
	URL   string        `json:"url"`
	UUID  string        `json:"uuid"`
	Token config.Secret `json:"token"`
}

func (c *forgeConfig) connection(ctx context.Context) (bridge.Connection, error) {
	u, err := url.ParseRequestURI(c.URL)
	if err != nil || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return bridge.Connection{}, fmt.Errorf("%w: invalid URL", provider.ErrInvalidConfig)
	}
	if c.Type != config.ProviderForgejo {
		return bridge.Connection{}, provider.ErrInvalidConfig
	}
	id, err := uuid.Parse(c.UUID)
	if err != nil || id == uuid.Nil {
		return bridge.Connection{}, fmt.Errorf("%w: invalid runner UUID", provider.ErrInvalidConfig)
	}
	token, err := c.Token.Take(ctx)
	if err != nil {
		return bridge.Connection{}, fmt.Errorf("%w: invalid runner token", provider.ErrInvalidConfig)
	}
	return bridge.Connection{URL: c.URL, UUID: c.UUID, Token: token}, nil
}

func main() {
	socket := flag.String("connect", "", "HRC parent socket")
	register := flag.String("register", "", "Register at instance URL; prints credentials JSON")
	name := flag.String("name", "hrc-forgejo-poc", "Registration name")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *register != "" {
		connection, err := bridge.Register(ctx, *register, *name, os.Getenv("FORGEJO_REGISTRATION_TOKEN"))
		if err != nil {
			slog.Error("Registration failed", "err", err)
			os.Exit(1)
		}
		data, err := json.Marshal(connection)
		if err != nil {
			os.Exit(1)
		}
		fmt.Println(string(data))
		return
	}
	if *socket == "" {
		flag.Usage()
		os.Exit(1)
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", *socket)
	if err == nil {
		client := &provider.QueueClient{New: func(start *provider.QueueStart) (provider.Queue, error) {
			var cfg forgeConfig
			if err := json.Unmarshal(start.Config, &cfg, json.RejectUnknownMembers(true)); err != nil {
				return nil, err
			}
			connection, err := cfg.connection(ctx)
			if err != nil {
				return nil, err
			}
			return &queue{connection: connection}, nil
		}}
		err = client.Connect(ctx, conn)
	}
	if err != nil {
		slog.Error("Forgejo queue stopped", "err", err)
		os.Exit(1)
	}
}
