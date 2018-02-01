package nomad

import (
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	metrics "github.com/armon/go-metrics"
	"github.com/hashicorp/nomad/acl"
	cstructs "github.com/hashicorp/nomad/client/structs"
	"github.com/hashicorp/nomad/helper"
	"github.com/hashicorp/nomad/nomad/structs"
	"github.com/ugorji/go/codec"
)

// TODO a Single RPC for "Cat", "ReadAt", "Stream" endpoints

// FileSystem endpoint is used for accessing the logs and filesystem of
// allocations from a Node.
type FileSystem struct {
	srv *Server
}

func (f *FileSystem) register() {
	f.srv.streamingRpcs.Register("FileSystem.Logs", f.logs)
}

func (f *FileSystem) handleStreamResultError(err error, code *int64, encoder *codec.Encoder) {
	// Nothing to do as the conn is closed
	if err == io.EOF || strings.Contains(err.Error(), "closed") {
		return
	}

	// Attempt to send the error
	encoder.Encode(&cstructs.StreamErrWrapper{
		Error: cstructs.NewRpcError(err, code),
	})
}

// List is used to list the contents of an allocation's directory.
func (f *FileSystem) List(args *cstructs.FsListRequest, reply *cstructs.FsListResponse) error {
	// We only allow stale reads since the only potentially stale information is
	// the Node registration and the cost is fairly high for adding another hope
	// in the forwarding chain.
	args.QueryOptions.AllowStale = true

	// Potentially forward to a different region.
	if done, err := f.srv.forward("FileSystem.List", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"nomad", "file_system", "list"}, time.Now())

	// Check filesystem read permissions
	if aclObj, err := f.srv.ResolveToken(args.AuthToken); err != nil {
		return err
	} else if aclObj != nil && !aclObj.AllowNsOp(args.Namespace, acl.NamespaceCapabilityReadFS) {
		return structs.ErrPermissionDenied
	}

	// Verify the arguments.
	if args.AllocID == "" {
		return errors.New("missing allocation ID")
	}

	// Lookup the allocation
	snap, err := f.srv.State().Snapshot()
	if err != nil {
		return err
	}

	alloc, err := snap.AllocByID(nil, args.AllocID)
	if err != nil {
		return err
	}
	if alloc == nil {
		return fmt.Errorf("unknown allocation %q", args.AllocID)
	}

	// Get the connection to the client
	state, ok := f.srv.getNodeConn(alloc.NodeID)
	if !ok {
		node, err := snap.NodeByID(nil, alloc.NodeID)
		if err != nil {
			return err
		}

		if node == nil {
			return fmt.Errorf("Unknown node %q", alloc.NodeID)
		}

		// Determine the Server that has a connection to the node.
		srv, err := f.srv.serverWithNodeConn(alloc.NodeID, f.srv.Region())
		if err != nil {
			return err
		}

		if srv == nil {
			return structs.ErrNoNodeConn
		}

		return f.srv.forwardServer(srv, "FileSystem.List", args, reply)
	}

	// Make the RPC
	return NodeRpc(state.Session, "FileSystem.List", args, reply)
}

// Stat is used to stat a file in the allocation's directory.
func (f *FileSystem) Stat(args *cstructs.FsStatRequest, reply *cstructs.FsStatResponse) error {
	// We only allow stale reads since the only potentially stale information is
	// the Node registration and the cost is fairly high for adding another hope
	// in the forwarding chain.
	args.QueryOptions.AllowStale = true

	// Potentially forward to a different region.
	if done, err := f.srv.forward("FileSystem.Stat", args, args, reply); done {
		return err
	}
	defer metrics.MeasureSince([]string{"nomad", "file_system", "stat"}, time.Now())

	// Check filesystem read permissions
	if aclObj, err := f.srv.ResolveToken(args.AuthToken); err != nil {
		return err
	} else if aclObj != nil && !aclObj.AllowNsOp(args.Namespace, acl.NamespaceCapabilityReadFS) {
		return structs.ErrPermissionDenied
	}

	// Verify the arguments.
	if args.AllocID == "" {
		return errors.New("missing allocation ID")
	}

	// Lookup the allocation
	snap, err := f.srv.State().Snapshot()
	if err != nil {
		return err
	}

	alloc, err := snap.AllocByID(nil, args.AllocID)
	if err != nil {
		return err
	}
	if alloc == nil {
		return fmt.Errorf("unknown allocation %q", args.AllocID)
	}

	// Get the connection to the client
	state, ok := f.srv.getNodeConn(alloc.NodeID)
	if !ok {
		node, err := snap.NodeByID(nil, alloc.NodeID)
		if err != nil {
			return err
		}

		if node == nil {
			return fmt.Errorf("Unknown node %q", alloc.NodeID)
		}

		// Determine the Server that has a connection to the node.
		srv, err := f.srv.serverWithNodeConn(alloc.NodeID, f.srv.Region())
		if err != nil {
			return err
		}

		if srv == nil {
			return structs.ErrNoNodeConn
		}

		return f.srv.forwardServer(srv, "FileSystem.Stat", args, reply)
	}

	// Make the RPC
	return NodeRpc(state.Session, "FileSystem.Stat", args, reply)
}

// Stats is used to retrieve the Clients stats.
func (f *FileSystem) logs(conn io.ReadWriteCloser) {
	defer conn.Close()
	defer metrics.MeasureSince([]string{"nomad", "file_system", "logs"}, time.Now())

	// Decode the arguments
	var args cstructs.FsLogsRequest
	decoder := codec.NewDecoder(conn, structs.MsgpackHandle)
	encoder := codec.NewEncoder(conn, structs.MsgpackHandle)

	if err := decoder.Decode(&args); err != nil {
		f.handleStreamResultError(err, helper.Int64ToPtr(500), encoder)
		return
	}

	// Check if we need to forward to a different region
	if r := args.RequestRegion(); r != f.srv.Region() {
		// Request the allocation from the target region
		allocReq := &structs.AllocSpecificRequest{
			AllocID:      args.AllocID,
			QueryOptions: args.QueryOptions,
		}
		var allocResp structs.SingleAllocResponse
		if err := f.srv.forwardRegion(r, "Alloc.GetAlloc", allocReq, &allocResp); err != nil {
			f.handleStreamResultError(err, nil, encoder)
			return
		}

		if allocResp.Alloc == nil {
			f.handleStreamResultError(fmt.Errorf("unknown allocation %q", args.AllocID), nil, encoder)
			return
		}

		// Determine the Server that has a connection to the node.
		srv, err := f.srv.serverWithNodeConn(allocResp.Alloc.NodeID, r)
		if err != nil {
			f.handleStreamResultError(err, nil, encoder)
			return
		}

		// Get a connection to the server
		srvConn, err := f.srv.streamingRpc(srv, "FileSystem.Logs")
		if err != nil {
			f.handleStreamResultError(err, nil, encoder)
			return
		}
		defer srvConn.Close()

		// Send the request.
		outEncoder := codec.NewEncoder(srvConn, structs.MsgpackHandle)
		if err := outEncoder.Encode(args); err != nil {
			f.handleStreamResultError(err, nil, encoder)
			return
		}

		structs.Bridge(conn, srvConn)
		return

	}

	// Check node read permissions
	if aclObj, err := f.srv.ResolveToken(args.AuthToken); err != nil {
		f.handleStreamResultError(err, nil, encoder)
		return
	} else if aclObj != nil {
		readfs := aclObj.AllowNsOp(args.QueryOptions.Namespace, acl.NamespaceCapabilityReadFS)
		logs := aclObj.AllowNsOp(args.QueryOptions.Namespace, acl.NamespaceCapabilityReadLogs)
		if !readfs && !logs {
			f.handleStreamResultError(structs.ErrPermissionDenied, nil, encoder)
			return
		}
	}

	// Verify the arguments.
	if args.AllocID == "" {
		f.handleStreamResultError(errors.New("missing AllocID"), helper.Int64ToPtr(400), encoder)
		return
	}

	// Retrieve the allocation
	snap, err := f.srv.State().Snapshot()
	if err != nil {
		f.handleStreamResultError(err, nil, encoder)
		return
	}

	alloc, err := snap.AllocByID(nil, args.AllocID)
	if err != nil {
		f.handleStreamResultError(err, nil, encoder)
		return
	}
	if alloc == nil {
		f.handleStreamResultError(fmt.Errorf("unknown alloc ID %q", args.AllocID), helper.Int64ToPtr(404), encoder)
		return
	}
	nodeID := alloc.NodeID

	// Get the connection to the client either by forwarding to another server
	// or creating a direct stream
	var clientConn net.Conn
	state, ok := f.srv.getNodeConn(nodeID)
	if !ok {
		// Determine the Server that has a connection to the node.
		srv, err := f.srv.serverWithNodeConn(nodeID, f.srv.Region())
		if err != nil {
			f.handleStreamResultError(err, nil, encoder)
			return
		}

		// Get a connection to the server
		conn, err := f.srv.streamingRpc(srv, "FileSystem.Logs")
		if err != nil {
			f.handleStreamResultError(err, nil, encoder)
			return
		}

		clientConn = conn
	} else {
		stream, err := NodeStreamingRpc(state.Session, "FileSystem.Logs")
		if err != nil {
			f.handleStreamResultError(err, nil, encoder)
			return
		}
		clientConn = stream
	}
	defer clientConn.Close()

	// Send the request.
	outEncoder := codec.NewEncoder(clientConn, structs.MsgpackHandle)
	if err := outEncoder.Encode(args); err != nil {
		f.handleStreamResultError(err, nil, encoder)
		return
	}

	structs.Bridge(conn, clientConn)
	return
}
