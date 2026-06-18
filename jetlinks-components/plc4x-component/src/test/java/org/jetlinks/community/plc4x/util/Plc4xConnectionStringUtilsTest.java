package org.jetlinks.community.plc4x.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class Plc4xConnectionStringUtilsTest {

    @Test
    void shouldKeepBaseUrlWhenUsernameIsBlank() {
        String connectionString = "opcua:tcp://127.0.0.1:49320?username=old&security-policy=None";

        String result = Plc4xConnectionStringUtils.withAuthentication(connectionString, " ", "new");

        assertEquals(connectionString, result);
    }

    @Test
    void shouldReplaceAuthenticationAtBeginningWithoutCorruptingRemainingParameters() {
        String connectionString = "opcua:tcp://127.0.0.1:49320?username=old&security-policy=None&password=old";

        String result = Plc4xConnectionStringUtils.withAuthentication(connectionString, "admin", "p@ss word");

        assertEquals(
                "opcua:tcp://127.0.0.1:49320?security-policy=None&username=admin&password=p%40ss+word",
                result);
    }

    @Test
    void shouldReplaceAuthenticationInMiddleWithoutDroppingOtherParameters() {
        String connectionString = "s7://192.168.1.10?remote-rack=0&username=old&remote-slot=1";

        String result = Plc4xConnectionStringUtils.withAuthentication(connectionString, "admin", "secret");

        assertEquals("s7://192.168.1.10?remote-rack=0&remote-slot=1&username=admin&password=secret", result);
    }

    @Test
    void shouldRemoveOldPasswordWhenNewPasswordIsBlank() {
        String connectionString = "opcua:tcp://127.0.0.1:49320?password=old&security-policy=None";

        String result = Plc4xConnectionStringUtils.withAuthentication(connectionString, "admin", "");

        assertEquals("opcua:tcp://127.0.0.1:49320?security-policy=None&username=admin", result);
    }
}
