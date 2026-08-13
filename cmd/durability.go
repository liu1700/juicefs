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
	"encoding/json"
	"errors"
	"fmt"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/utils"
	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/urfave/cli/v2"
)

var errDurabilityUnsupported = errors.New("remote durability barrier is not supported by the mounted client")

func cmdDurability() *cli.Command {
	return &cli.Command{
		Name:      "durability",
		Action:    durability,
		Category:  "SERVICE",
		Usage:     "Wait for writeback data to reach object storage",
		ArgsUsage: "MOUNTPOINT",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:  "status",
				Usage: "show current writeback durability status without waiting",
			},
			&cli.DurationFlag{
				Name:  "timeout",
				Value: 10 * time.Minute,
				Usage: "maximum time to wait for the durability barrier (0 means no timeout)",
			},
			&cli.BoolFlag{
				Name:  "json",
				Usage: "print the response as JSON",
			},
		},
	}
}

func requestRemoteDurability(mountpoint string, statusOnly bool, timeout time.Duration) (*vfs.DurabilityResponse, error) {
	if timeout < 0 {
		return nil, errors.New("durability timeout must not be negative")
	}
	f, err := openController(mountpoint)
	if err != nil {
		return nil, fmt.Errorf("open control file: %w", err)
	}
	defer f.Close()

	bodyLen := uint32(1)
	if !statusOnly {
		bodyLen += 8
	}
	w := utils.NewBuffer(8 + bodyLen)
	w.Put32(meta.RemoteDurability)
	w.Put32(bodyLen)
	if statusOnly {
		w.Put8(1)
	} else {
		w.Put8(0)
		wireTimeout := uint64(timeout / time.Millisecond)
		if timeout%time.Millisecond != 0 {
			wireTimeout++
		}
		w.Put64(wireTimeout)
	}
	if _, err = f.Write(w.Bytes()); err != nil {
		return nil, fmt.Errorf("write durability request: %w", err)
	}
	data, errno := readProgress(f, func(uint64, uint64) {})
	if errno == syscall.EINVAL || errno == syscall.EOPNOTSUPP {
		return nil, errDurabilityUnsupported
	}
	if errno != 0 {
		return nil, fmt.Errorf("durability request: %w", errno)
	}
	var resp vfs.DurabilityResponse
	if err = json.Unmarshal(data, &resp); err != nil {
		return nil, fmt.Errorf("decode durability response: %w", err)
	}
	if resp.Error != "" {
		return &resp, errors.New(resp.Error)
	}
	return &resp, nil
}

func durability(ctx *cli.Context) error {
	setup0(ctx, 1, 0)
	resp, err := requestRemoteDurability(ctx.Args().Get(0), ctx.Bool("status"), ctx.Duration("timeout"))
	if err != nil {
		return err
	}
	if ctx.Bool("json") {
		data, _ := json.Marshal(resp)
		fmt.Println(string(data))
		return nil
	}
	fmt.Printf("fence: %d\n", resp.Fence)
	fmt.Printf("pending blocks: %d\n", resp.PendingBlocks)
	fmt.Printf("pending bytes: %s\n", utils.FormatBytes(resp.PendingBytes))
	fmt.Printf("oldest pending age: %s\n", time.Duration(resp.OldestPendingAgeMillis)*time.Millisecond)
	fmt.Printf("failed uploads: %d\n", resp.FailedUploads)
	if resp.LastError != "" {
		fmt.Printf("last error: %s\n", resp.LastError)
	}
	if resp.LastSuccessfulBarrierUnixMs != 0 {
		fmt.Printf("last successful barrier: %s (fence %d)\n",
			time.UnixMilli(resp.LastSuccessfulBarrierUnixMs).Format(time.RFC3339), resp.LastSuccessfulFence)
	}
	return nil
}
