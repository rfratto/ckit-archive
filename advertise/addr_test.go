package advertise_test

import (
	"net"
	"strings"
	"testing"

	"github.com/rfratto/ckit/advertise"
	"github.com/stretchr/testify/require"
)

// TestDefaultInterfaces ensures that there's at least one interface in
// DefaultInterfaces that exists on the host OS performing the testing.
func TestDefaultInterfaces(t *testing.T) {
	// Exit the test if we find at least one interface from the default set.
	for _, name := range advertise.DefaultInterfaces {
		iface, err := net.InterfaceByName(name)
		if err == nil && iface != nil {
			return
		}
	}

	// We didn't find an interface. Find the available ones.
	ifaces, err := net.Interfaces()
	require.NoError(t, err, "failed to list interfaces")

	ifaceNames := make([]string, len(ifaces))
	for i, iface := range ifaces {
		ifaceNames[i] = iface.Name
	}

	require.FailNow(t,
		"DefaultInterfaces does not include any available interface",
		"Available interfaces: %s", strings.Join(ifaceNames, ", "),
	)
}
