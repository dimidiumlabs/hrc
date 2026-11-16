// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package config

import (
	"bytes"
	"encoding/json/v2"
	"errors"
	"fmt"
	"reflect"
)

// Sum holds one configuration variant selected by its "type" field.
// T is a struct of pointers to configuration types, tagged with variant names.
// Each variant must directly embed the exported struct B as a value field.
// If T has just one variant, an omitted type selects it.
// Parsing checks syntax and unknown fields; Validate applies semantic rules.
type Sum[B, T any] struct {
	value interface{ Validate() error }
	base  *B
}

func (s Sum[B, T]) Value() any { return s.value }

// Base returns the embedded base of the active variant, or nil before decoding.
func (s Sum[B, T]) Base() *B { return s.base }

func (s Sum[B, T]) Validate() error {
	if s.value == nil {
		return errors.New("missing configuration variant")
	}
	return s.value.Validate()
}

func (s *Sum[B, T]) UnmarshalJSON(data []byte) error {
	data = bytes.TrimSpace(data)
	if len(data) == 0 || data[0] != '{' {
		return errors.New("configuration must be an object")
	}
	var header struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(data, &header); err != nil {
		return err
	}
	spec := reflect.TypeFor[T]()
	if spec.Kind() != reflect.Struct {
		return errors.New("sum variants must be a struct")
	}
	baseType := reflect.TypeFor[B]()
	if baseType.Kind() != reflect.Struct {
		return errors.New("sum base must be a struct")
	}
	var selected reflect.Type
	selectedBase := -1
	names := make(map[string]bool, spec.NumField())
	validator := reflect.TypeFor[interface{ Validate() error }]()
	for i := 0; i < spec.NumField(); i++ {
		field := spec.Field(i)
		name := field.Tag.Get("variant")
		if !field.IsExported() || name == "" || names[name] || field.Type.Kind() != reflect.Pointer || field.Type.Elem().Kind() != reflect.Struct || !field.Type.Implements(validator) {
			return fmt.Errorf("invalid sum variant %s", field.Name)
		}
		baseIndex := -1
		variant := field.Type.Elem()
		for j := 0; j < variant.NumField(); j++ {
			candidate := variant.Field(j)
			if candidate.Anonymous && candidate.IsExported() && candidate.Type == baseType {
				if baseIndex >= 0 {
					return fmt.Errorf("ambiguous base in sum variant %s", field.Name)
				}
				baseIndex = j
			}
		}
		if baseIndex < 0 {
			return fmt.Errorf("sum variant %s must embed %s", field.Name, baseType)
		}
		names[name] = true
		if name == header.Type || (header.Type == "" && spec.NumField() == 1) {
			selected = field.Type
			selectedBase = baseIndex
		}
	}
	if selected == nil {
		return fmt.Errorf("unknown configuration type %q", header.Type)
	}
	instance := reflect.New(selected.Elem())
	value := instance.Interface().(interface{ Validate() error })
	if err := json.Unmarshal(data, value, json.RejectUnknownMembers(true)); err != nil {
		return err
	}
	// Keep the previous value intact if selecting or decoding a variant fails.
	s.value, s.base = value, instance.Elem().Field(selectedBase).Addr().Interface().(*B)
	return nil
}

func (s Sum[B, T]) MarshalJSON() ([]byte, error) {
	if s.value == nil {
		return nil, errors.New("missing configuration variant")
	}
	return json.Marshal(s.value)
}
