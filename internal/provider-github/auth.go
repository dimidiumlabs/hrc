// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/actions/scaleset"
	"github.com/golang-jwt/jwt/v5"

	"dimidiumlabs.io/hrc/internal/config"
)

func (c *ForgeConfig) newClient(info scaleset.SystemInfo) (*scaleset.Client, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	if c.GitHubApp.Validate() == nil {
		app := &c.GitHubApp
		return scaleset.NewClientWithJWTProvider(scaleset.ClientWithJWTProviderConfig{
			GitHubConfigURL: c.Url, InstallationID: app.InstallationID, SystemInfo: info,
		}, scaleset.JWTProviderFunc(func(ctx context.Context) (string, error) {
			value, err := app.PrivateKey.Take(ctx)
			if err != nil {
				return "", fmt.Errorf("read private key: %w", err)
			}
			key, err := jwt.ParseRSAPrivateKeyFromPEM([]byte(value))
			if err != nil {
				return "", fmt.Errorf("invalid GitHub App RSA private key")
			}
			provider := scaleset.SignerJWTProvider{ClientID: app.ClientID, Signer: key}
			return provider.Token(ctx)
		}))
	}
	if c.GitHubJWT.Validate() == nil {
		return scaleset.NewClientWithJWTProvider(scaleset.ClientWithJWTProviderConfig{
			GitHubConfigURL: c.Url, InstallationID: c.GitHubJWT.InstallationID, SystemInfo: info,
		}, &secretJWTProvider{secret: &c.GitHubJWT.Token})
	}
	return scaleset.NewClientWithPersonalAccessToken(scaleset.NewClientWithPersonalAccessTokenConfig{
		GitHubConfigURL: c.Url, PersonalAccessToken: patPlaceholder, SystemInfo: info,
	}, scaleset.WithHTTPClient(&secretPATClient{
		secret: &c.Token, next: scaleset.NewHTTPClient(scaleset.HTTPClientConfig{}),
	}))
}

type secretJWTProvider struct {
	secret *config.Secret
}

func (p *secretJWTProvider) Token(ctx context.Context) (string, error) {
	for attempt := 0; attempt < 2; attempt++ {
		value, err := p.secret.Take(ctx)
		if err != nil {
			return "", fmt.Errorf("read GitHub App JWT: %w", err)
		}
		claims := &jwt.RegisteredClaims{}
		token, _, err := jwt.NewParser().ParseUnverified(value, claims)
		if err != nil || token.Method.Alg() != "RS256" || len(token.Signature) == 0 || claims.Issuer == "" || claims.IssuedAt == nil || claims.ExpiresAt == nil {
			p.secret.Invalidate()
			return "", fmt.Errorf("invalid GitHub App JWT: RS256, signature, iss, iat and exp are required")
		}
		if claims.IssuedAt.Time.After(time.Now().Add(time.Minute)) {
			p.secret.Invalidate()
			return "", fmt.Errorf("GitHub App JWT issued in the future")
		}
		if claims.ExpiresAt.Time.After(time.Now().Add(30 * time.Second)) {
			return value, nil
		}
		p.secret.Invalidate()
	}
	return "", fmt.Errorf("GitHub App JWT expires in less than 30 seconds")
}

const patPlaceholder = "hrc-dynamic-pat-placeholder"

type secretPATClient struct {
	secret *config.Secret
	next   scaleset.HTTPClient
}

func (c *secretPATClient) Do(req *http.Request) (*http.Response, error) {
	if req.Header.Get("Authorization") == "Bearer "+patPlaceholder {
		value, err := c.secret.Take(req.Context())
		if err != nil {
			return nil, fmt.Errorf("read PAT: %w", err)
		}
		req = req.Clone(req.Context())
		req.Header.Set("Authorization", "Bearer "+value)
	}
	return c.next.Do(req)
}
