//go:build plori
// +build plori

/*
 * JuiceFS, Copyright 2026 Juicedata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cmd

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"syscall"

	"github.com/google/uuid"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/plori/gatewaycontrol"
)

// CloneTree is the workspace gateway's one metadata mutation endpoint. The
// supervisor authenticates and fences the request before it reaches this
// method. This method deliberately accepts only the gateway's private trees;
// it is not a general metadata or .control operation bridge.
func (p *ploriVolume) CloneTree(ctx context.Context, req gatewaycontrol.CloneRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if p.v == nil || p.v.Meta == nil {
		return errors.New("workspace clone: mount is not serving")
	}
	source, destinationParent, destinationName, err := workspaceClonePaths(req.Source, req.Destination)
	if err != nil {
		return err
	}
	if req.SourceNativeInode == 0 {
		return errors.New("workspace clone: source native inode is required")
	}

	metaCtx := meta.WrapContext(ctx)
	defer metaCtx.Cancel()
	sourceParent, err := p.workspaceDirectory(metaCtx, source[:len(source)-1])
	if err != nil {
		return fmt.Errorf("workspace clone: resolve source parent: %w", err)
	}
	sourceInode, err := p.workspaceDirectory(metaCtx, source)
	if err != nil {
		return fmt.Errorf("workspace clone: resolve source: %w", err)
	}
	if uint64(sourceInode) != req.SourceNativeInode {
		return errors.New("workspace clone: source native inode differs")
	}
	destinationInode, err := p.workspaceDirectory(metaCtx, destinationParent)
	if err != nil {
		return fmt.Errorf("workspace clone: resolve destination parent: %w", err)
	}
	if err := p.workspaceCloneDestinationAbsent(metaCtx, destinationInode, destinationName); err != nil {
		return err
	}

	var count, total uint64
	if st := p.v.Meta.Clone(metaCtx, sourceParent, sourceInode, destinationInode, destinationName,
		meta.CLONE_MODE_PRESERVE_ATTR, 0, meta.CLONE_DEFAULT_CONCURRENCY, &count, &total); st != 0 {
		return fmt.Errorf("workspace clone: %w", st)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

// workspaceDirectory resolves only directories from the mount's RootInode.
// Metadata lookup makes a symlink fail the type check instead of following it.
func (p *ploriVolume) workspaceDirectory(ctx meta.Context, parts []string) (meta.Ino, error) {
	inode := meta.RootInode
	for _, name := range parts {
		var child meta.Ino
		var attr meta.Attr
		if st := p.v.Meta.Lookup(ctx, inode, name, &child, &attr, true); st != 0 {
			return 0, st
		}
		if attr.Typ != meta.TypeDirectory {
			return 0, fmt.Errorf("%s is not a directory", name)
		}
		inode = child
	}
	return inode, nil
}

func (p *ploriVolume) workspaceCloneDestinationAbsent(ctx meta.Context, parent meta.Ino, name string) error {
	var inode meta.Ino
	var attr meta.Attr
	switch st := p.v.Meta.Lookup(ctx, parent, name, &inode, &attr, true); st {
	case 0:
		return errors.New("workspace clone: destination exists")
	case syscall.ENOENT:
		return nil
	default:
		return fmt.Errorf("workspace clone: resolve destination: %w", st)
	}
}

func workspaceClonePaths(source, destination string) ([]string, []string, string, error) {
	sourceParts, ok := workspaceControlSegments(source)
	if !ok {
		return nil, nil, "", errors.New("workspace clone: source path is not canonical")
	}
	switch {
	case len(sourceParts) == 2 && sourceParts[0] == "copies" && workspaceControlUUID(sourceParts[1]):
	case len(sourceParts) == 3 && sourceParts[0] == ".plori-gateway" && sourceParts[1] == "revisions" && workspaceControlUUID(sourceParts[2]):
	default:
		return nil, nil, "", errors.New("workspace clone: source is outside the allowed trees")
	}

	destinationParts, ok := workspaceControlSegments(destination)
	if !ok || len(destinationParts) != 3 || destinationParts[0] != ".plori-gateway" || destinationParts[1] != "staging" {
		return nil, nil, "", errors.New("workspace clone: destination is outside the staging tree")
	}
	name := destinationParts[2]
	if !(strings.HasPrefix(name, "copy-") && workspaceControlUUID(strings.TrimPrefix(name, "copy-"))) &&
		!(strings.HasPrefix(name, "revision-") && workspaceControlUUID(strings.TrimPrefix(name, "revision-"))) {
		return nil, nil, "", errors.New("workspace clone: destination name is not a typed operation UUID")
	}
	return sourceParts, destinationParts[:2], name, nil
}

func workspaceControlSegments(path string) ([]string, bool) {
	if path == "" || strings.HasPrefix(path, "/") || strings.ContainsAny(path, "\\\x00") {
		return nil, false
	}
	parts := strings.Split(path, "/")
	for _, part := range parts {
		if part == "" || part == "." || part == ".." {
			return nil, false
		}
	}
	return parts, true
}

func workspaceControlUUID(raw string) bool {
	id, err := uuid.Parse(raw)
	return err == nil && id != uuid.Nil && id.String() == raw
}
