package eventtransport

import "fmt"

func (c *Client) SendError(code, message string) {
	errorMsg := EventError{
		Code:    code,
		Message: message,
	}

	err := c.SendMessage(errorMsg)
	if err != nil {
		fmt.Printf("[ERROR] [ACK] %s\n", err)
	}

}

func (c *Client) SendAck(payload EventMessagePayload) {
	err := c.SendMessage(payload)
	if err != nil {
		fmt.Printf("[ERROR] [ACK] %s\n", err)
	}
}
