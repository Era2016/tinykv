package server

import (
	"context"
	"fmt"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	var resp kvrpcpb.RawGetResponse
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		resp.Error = fmt.Sprintf("%v", err)
		return &resp, err
	}

	val, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		resp.Error = fmt.Sprintf("%v", err)
		return &resp, err
	}

	if val == nil {
		resp.NotFound = true
	}
	resp.Value = val
	return &resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	var resp kvrpcpb.RawPutResponse
	m := storage.Modify{
		Data: storage.Put{
			Key:   req.Key,
			Value: req.Value,
			Cf:    req.Cf,
		},
	}

	err := server.storage.Write(req.Context, []storage.Modify{m})
	resp.Error = fmt.Sprintf("%v", err)
	return &resp, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	var resp kvrpcpb.RawDeleteResponse
	m := storage.Modify{
		Data: storage.Delete{
			Key: req.Key,
			Cf:  req.Cf,
		},
	}

	err := server.storage.Write(req.Context, []storage.Modify{m})
	resp.Error = fmt.Sprintf("%v", err)
	return &resp, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	var resp kvrpcpb.RawScanResponse
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		resp.Error = fmt.Sprintf("%v", reader)
		return &resp, nil
	}

	kvs := make([]*kvrpcpb.KvPair, 0)
	iter := reader.IterCF(req.Cf)
	iter.Seek(req.StartKey)
	defer iter.Close()

	index := 0
	for iter.Valid() && index < int(req.Limit) {
		val, err := iter.Item().Value()
		kvs = append(kvs, &kvrpcpb.KvPair{
			Error: &kvrpcpb.KeyError{
				Retryable: fmt.Sprintf("%v", err),
			},
			Key:   iter.Item().Key(),
			Value: val,
		})
		index++
		iter.Next()
	}

	resp.Kvs = kvs
	return &resp, nil
}
