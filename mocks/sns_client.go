// Code generated by mockery v1.0.0. DO NOT EDIT.

package mocks

import mock "github.com/stretchr/testify/mock"

// SNSClient is an autogenerated mock type for the SNSClient type
type SNSClient struct {
	mock.Mock
}

// publish provides a mock function with given fields: data
func (_m *SNSClient) Publish(data string) error {
	ret := _m.Called(data)

	var r0 error
	if rf, ok := ret.Get(0).(func(string) error); ok {
		r0 = rf(data)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}
