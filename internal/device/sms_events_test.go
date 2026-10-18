package device

import "testing"

func TestSignalSMSArrivalOnlyForCMTI(t *testing.T) {
	arrivals := make(chan struct{}, 1)
	handle := signalSMSArrival(arrivals)

	// Only +CMTI announces a stored SMS under the CNMI mode VoCat configures.
	for _, line := range []string{"+CEREG: 1", `+CMT: "+447700900123",,"26/10/18,10:00:00+00"`, `+CDSI: "SM",3`} {
		handle(line)
	}
	select {
	case <-arrivals:
		t.Fatal("non-+CMTI URC signalled an SMS arrival")
	default:
	}
	handle(`+CMTI: "SM",5`)
	handle(`+CMTI: "SM",6`)
	select {
	case <-arrivals:
	default:
		t.Fatal("+CMTI did not signal an SMS arrival")
	}
	// The burst collapses into the single wake-up already consumed.
	select {
	case <-arrivals:
		t.Fatal("+CMTI burst produced more than one wake-up")
	default:
	}
}
