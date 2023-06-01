package pandaproxy

import (
	"context"
	"github.com/goccy/go-json"
	"io"
	"net/http"
	"time"
)

func GetBrokers(baseUrl string) (*Brokers, *ErrorBody, error) {
	response, err := DoRequest(http.MethodGet, baseUrl+"/brokers", nil)
	if err != nil {
		return nil, nil, err
	}

	var bodyBytes []byte
	// read all response body bytes
	bodyBytes, err = io.ReadAll(response.Body)
	if err != nil {
		return nil, nil, err
	}

	if response.StatusCode != http.StatusOK {
		// Parse error body
		var errorBody ErrorBody
		err = json.Unmarshal(bodyBytes, &errorBody)
		if err != nil {
			return nil, nil, err
		}
		return nil, &errorBody, nil
	}
	var brokers Brokers
	err = json.Unmarshal(bodyBytes, &brokers)
	if err != nil {
		return nil, nil, err
	}

	return &brokers, nil, nil
}

func DoRequest(method, url string, body io.Reader) (*http.Response, error) {
	ctx, cancle := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancle()
	request, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Accept", "application/vnd.kafka.v2+json")
	request.Header.Set("Content-Type", "application/vnd.kafka.v2+json")

	var response *http.Response
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		return nil, err
	}
	return response, nil
}
