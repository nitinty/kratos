package kratos

import (
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/xmidt-org/wrp-go/v3"
	"go.uber.org/zap"
)

// Client is what function calls we expose to the user of kratos
type Client interface {
	Hostname() string
	HandlerRegistry() HandlerRegistry
	Send(message *wrp.Message)
	Close() error
}

// sendWRPFunc is the function for sending a message downstream.
type sendWRPFunc func(*wrp.Message)

type client struct {
	deviceID        string
	userAgent       string
	deviceProtocols string
	hostname        string
	registry        HandlerRegistry
	handlePingMiss  HandlePingMiss
	encoderSender   encoderSender
	decoderSender   decoderSender
	connection      websocketConnection
	headerInfo      *clientHeader
	logger          *zap.Logger
	done            chan struct{}
	wg              sync.WaitGroup
	pingConfig      PingConfig
	once            sync.Once
	config          ClientConfig
	pinged          chan string
	connMu          sync.Mutex
}

// used to track everything that we want to know about the client headers
type clientHeader struct {
	deviceName   string
	firmwareName string
	modelName    string
	manufacturer string
	token        string
}

// websocketConnection maintains the websocket connection upstream (to XMiDT).
type websocketConnection interface {
	WriteMessage(messageType int, data []byte) error
	ReadMessage() (messageType int, p []byte, err error)
	Close() error
}

// Hostname provides the client's hostname.
func (c *client) Hostname() string {
	return c.hostname
}

// HandlerRegistry returns the HandlerRegistry that the client maintains.
func (c *client) HandlerRegistry() HandlerRegistry {
	return c.registry
}

// Send is used to open a channel for writing to XMiDT
func (c *client) Send(message *wrp.Message) {
	c.encoderSender.EncodeAndSend(message)
}

// Close closes connections downstream and the socket upstream.
func (c *client) Close() error {
	var connectionErr error
	c.once.Do(func() {
		c.logger.Info("Closing client...")
		close(c.done)
		c.wg.Wait()
		c.decoderSender.Close()
		c.encoderSender.Close()
		connectionErr = c.connection.Close()
		c.connection = nil
		// TODO: if this fails, can we really do anything. Is there potential for leaks?
		// if err != nil {
		// 	return emperror.Wrap(err, "Failed to close connection")
		// }
		c.logger.Info("Client Closed")
	})
	return connectionErr
}

// // going to be used to access the HandleMessage() function
// func (c *client) read() {
// 	defer c.wg.Done()
// 	c.logger.Info("Watching socket for messages.")

// 	for {
// 		select {
// 		case <-c.done:
// 			c.logger.Info("Stopped reading from socket.")
// 			return
// 		default:
// 			c.logger.Info("Reading message...")

// 			_, serverMessage, err := c.connection.ReadMessage()
// 			if err != nil {
// 				c.logger.Error("Failed to read message. Exiting out of read loop.", zap.Error(err))
// 				return
// 			}
// 			c.decoderSender.DecodeAndSend(serverMessage)

// 			c.logger.Debug("Message sent to be decoded")
// 		}
// 	}
// }

// read continually listens for messages arriving on the websocket
// connection. If the connection closes or an error occurs, it attempts
// to reconnect by calling attemptReconnect().
func (c *client) read() {
	defer c.wg.Done() // mark this goroutine as done when we exit
	c.logger.Info("Watching socket for messages.")

	for {
		select {
		case <-c.done:
			// The client is shutting down. Stop reading messages.
			c.logger.Info("Stopped reading from socket.")
			return
		default:
			// No stop signal yet; continue reading.
			c.logger.Debug("Reading message...")
		}

		// Safely take a snapshot of the current websocket connection.
		// We lock only long enough to read the pointer.
		c.connMu.Lock()
		conn := c.connection
		c.connMu.Unlock()

		// If the connection pointer is nil, we’ve lost the connection.
		// Try to reconnect before attempting to read again.
		if conn == nil {
			c.logger.Warn("connection is nil, attempting reconnect")
			c.attemptReconnect()
			continue
		}

		// Block waiting for the next message from the server.
		_, serverMessage, err := conn.ReadMessage()
		if err != nil {
			// An error means the socket has failed or closed.
			// Log the failure and attempt to reconnect.
			c.logger.Error("Failed to read message. Will attempt to reconnect.", zap.Error(err))
			c.attemptReconnect()
			continue
		}

		// Successfully received a message: pass it to the decoder,
		// which will dispatch it to the appropriate handler.
		c.decoderSender.DecodeAndSend(serverMessage)
		c.logger.Debug("Message sent to be decoded")
	}
}

// attemptReconnect keeps trying to establish a new websocket connection
// if the current one is lost. It uses an exponential backoff so that
// repeated failures do not overwhelm the remote server.
func (c *client) attemptReconnect() {
	time.Sleep(180 * time.Second) // start with a 180-second delay between attempts

	backoff := 1 * time.Second
	for {
		select {
		case <-c.done:
			// If the client is shutting down, stop trying to reconnect.
			c.logger.Info("Stop requested; aborting reconnect attempts")
			return
		default:
			// No shutdown signal; continue trying to reconnect.
		}

		c.logger.Info("Trying to reconnect websocket...")

		// Recreate the websocket connection using the original headers
		// and configuration stored in the client.
		conn, wsURL, err := createConnection(c.headerInfo, c.config)
		if err != nil {
			// Connection attempt failed: log the error and wait before retrying.
			c.logger.Info("reconnect attempt failed", zap.Error(err), zap.Duration("retryIn", backoff))
			time.Sleep(backoff)

			// Exponential backoff: double the wait time up to a maximum of 30 seconds.
			if backoff < 30*time.Second {
				backoff *= 2
			}
			continue
		}

		// The websocket is successfully re-established.
		// Re-install the ping handler so that keep-alive pings from the server
		// will be answered and forwarded to the ping monitoring logic.
		conn.SetPingHandler(func(appData string) error {
			c.pinged <- appData
			return conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(writeWait))
		})

		// Safely swap the old connection for the new one.
		c.connMu.Lock()
		old := c.connection
		c.connection = conn
		c.connMu.Unlock()

		// Close the old connection (if there was one) after the swap so that
		// no other goroutine is still trying to read from it.
		if old != nil {
			_ = old.Close()
		}

		// Log the successful reconnection and exit the retry loop.
		c.logger.Info("websocket reconnected", zap.String("url", wsURL))
		return
	}
}
