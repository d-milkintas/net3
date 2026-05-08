// IN2011 Computer Networks
// Coursework 2024/2025
//
// Submission by
//  Dominykas Milkintas
//  240012813
//  Dominykas.Milkintas@citystgeorges.ac.uk


// DO NOT EDIT starts
// This gives the interface that your code must implement.
// These descriptions are intended to help you understand how the interface
// will be used. See the RFC for how the protocol works.

interface NodeInterface {

    /* These methods configure your node.
     * They must both be called once after the node has been created but
     * before it is used. */
    
    // Set the name of the node.
    public void setNodeName(String nodeName) throws Exception;

    // Open a UDP port for sending and receiving messages.
    public void openPort(int portNumber) throws Exception;


    /*
     * These methods query and change how the network is used.
     */

    // Handle all incoming messages.
    // If you wait for more than delay miliseconds and
    // there are no new incoming messages return.
    // If delay is zero then wait for an unlimited amount of time.
    public void handleIncomingMessages(int delay) throws Exception;
    
    // Determines if a node can be contacted and is responding correctly.
    // Handles any messages that have arrived.
    public boolean isActive(String nodeName) throws Exception;

    // You need to keep a stack of nodes that are used to relay messages.
    // The base of the stack is the first node to be used as a relay.
    // The first node must relay to the second node and so on.
    
    // Adds a node name to a stack of nodes used to relay all future messages.
    public void pushRelay(String nodeName) throws Exception;

    // Pops the top entry from the stack of nodes used for relaying.
    // No effect if the stack is empty
    public void popRelay() throws Exception;
    

    /*
     * These methods provide access to the basic functionality of
     * CRN-25 network.
     */

    // Checks if there is an entry in the network with the given key.
    // Handles any messages that have arrived.
    public boolean exists(String key) throws Exception;
    
    // Reads the entry stored in the network for key.
    // If there is a value, return it.
    // If there isn't a value, return null.
    // Handles any messages that have arrived.
    public String read(String key) throws Exception;

    // Sets key to be value.
    // Returns true if it worked, false if it didn't.
    // Handles any messages that have arrived.
    public boolean write(String key, String value) throws Exception;

    // If key is set to currentValue change it to newValue.
    // Returns true if it worked, false if it didn't.
    // Handles any messages that have arrived.
    public boolean CAS(String key, String currentValue, String newValue) throws Exception;

}
// DO NOT EDIT ends

// Complete this!
public class Node implements NodeInterface {

    private String nodeName;
    private int portNumber;
    private java.net.DatagramSocket socket;

    private java.util.Map<String, String> store = new java.util.HashMap<>();

    private int nextTransactionNumber = 0;

    private static final String TXID_CHARS =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    private byte[] computeHash(String text) throws Exception {
        return HashID.computeHashID(text);
    }

    private java.util.List<String> relayStack = new java.util.ArrayList<>();


    private String computeHashHex(String text) throws Exception {
        byte[] hash = computeHash(text);
        StringBuilder hex = new StringBuilder();

        for (byte b : hash) {
            hex.append(String.format("%02x", b & 0xff));
        }

        return hex.toString();
    }

    private int distance(String a, String b) throws Exception {
        byte[] hashA = computeHash(a);
        byte[] hashB = computeHash(b);

        return distanceBetweenHashes(hashA, hashB);
    }

    private int distanceBetweenHashes(byte[] hashA, byte[] hashB) {
        int matchingLeadingBits = 0;

        for (int i = 0; i < hashA.length; i++) {
            int byteA = hashA[i] & 0xff;
            int byteB = hashB[i] & 0xff;

            int xor = byteA ^ byteB;

            if (xor == 0) {
                matchingLeadingBits += 8;
            } else {
                for (int bit = 7; bit >= 0; bit--) {
                    int mask = 1 << bit;

                    if ((xor & mask) == 0) {
                        matchingLeadingBits++;
                    } else {
                        return 256 - matchingLeadingBits;
                    }
                }
            }
        }

        return 0;
    }

    private String encodeString(String text) {
        if (text == null) {
            text = "";
        }

        int spaces = 0;

        for (int i = 0; i < text.length(); i++) {
            if (text.charAt(i) == ' ') {
                spaces++;
            }
        }

        return spaces + " " + text + " ";
    }

    private static class DecodedString {
        String value;
        int nextIndex;

        DecodedString(String value, int nextIndex) {
            this.value = value;
            this.nextIndex = nextIndex;
        }
    }

    private DecodedString decodeString(String message, int startIndex) throws Exception {
        int i = startIndex;

        // Read the number of spaces
        StringBuilder numberText = new StringBuilder();

        while (i < message.length() && message.charAt(i) != ' ') {
            numberText.append(message.charAt(i));
            i++;
        }

        if (i >= message.length()) {
            throw new Exception("Bad CRN string: missing first space");
        }

        int expectedSpaces = Integer.parseInt(numberText.toString());

        // Skip the space after the number
        i++;

        int valueStart = i;
        int spacesSeenInsideString = 0;

        while (i < message.length()) {
            char c = message.charAt(i);

            if (c == ' ') {
                if (spacesSeenInsideString == expectedSpaces) {
                    String value = message.substring(valueStart, i);
                    return new DecodedString(value, i + 1);
                } else {
                    spacesSeenInsideString++;
                }
            }

            i++;
        }

        throw new Exception("Bad CRN string: missing final space");
    }


    public void setNodeName(String nodeName) throws Exception {
        if (nodeName == null || !nodeName.startsWith("N:")) {
            throw new Exception("Node name must start with N:");
        }

        this.nodeName = nodeName;
    }

    public void openPort(int portNumber) throws Exception {
        this.portNumber = portNumber;
        this.socket = new java.net.DatagramSocket(portNumber);

        // Store this node's own address key/value pair.
        // This is required by the RFC.
        if (this.nodeName != null) {
            String address = java.net.InetAddress.getLocalHost().getHostAddress() + ":" + portNumber;
            storeKeyValue(this.nodeName, address);
        }
    }

    public void handleIncomingMessages(int delay) throws Exception {
        if (socket == null) {
            throw new Exception("Port has not been opened yet.");
        }

        // delay == 0 means wait forever
        if (delay == 0) {
            socket.setSoTimeout(0);

            while (true) {
                try {
                    byte[] buffer = new byte[4096];
                    java.net.DatagramPacket packet =
                            new java.net.DatagramPacket(buffer, buffer.length);

                    socket.receive(packet);

                    String message = new String(
                            packet.getData(),
                            0,
                            packet.getLength(),
                            java.nio.charset.StandardCharsets.UTF_8
                    );

                    handleSingleMessage(message, packet.getAddress(), packet.getPort());

                } catch (Exception e) {
                    // Malformed packets should not crash the node.
                }
            }
        }

        // For non-zero delay, wait for roughly delay milliseconds total.
        long endTime = System.currentTimeMillis() + delay;

        while (System.currentTimeMillis() < endTime) {
            try {
                long remaining = endTime - System.currentTimeMillis();

                if (remaining <= 0) {
                    break;
                }

                socket.setSoTimeout((int) remaining);

                byte[] buffer = new byte[4096];
                java.net.DatagramPacket packet =
                        new java.net.DatagramPacket(buffer, buffer.length);

                socket.receive(packet);

                String message = new String(
                        packet.getData(),
                        0,
                        packet.getLength(),
                        java.nio.charset.StandardCharsets.UTF_8
                );

                handleSingleMessage(message, packet.getAddress(), packet.getPort());

            } catch (java.net.SocketTimeoutException e) {
                break;
            } catch (Exception e) {
                // Malformed packets should not crash the node.
            }
        }
    }


    private void handleSingleMessage(String message, java.net.InetAddress senderAddress, int senderPort) {
        try {
            if (message == null || message.length() < 4) {
                return;
            }

            String txid = message.substring(0, 2);

            if (txid.charAt(0) == ' ' || txid.charAt(1) == ' ') {
                return;
            }

            if (message.charAt(2) != ' ') {
                return;
            }

            char type = message.charAt(3);

            if (type == 'G') {
                handleNameRequest(txid, senderAddress, senderPort);
            } else if (type == 'N') {
                handleNearestRequest(txid, message, senderAddress, senderPort);
            } else if (type == 'E') {
                handleExistsRequest(txid, message, senderAddress, senderPort);
            } else if (type == 'R') {
                handleReadRequest(txid, message, senderAddress, senderPort);
            } else if (type == 'W') {
                handleWriteRequest(txid, message, senderAddress, senderPort);
            } else if (type == 'V') {
                handleRelayRequest(txid, message, senderAddress, senderPort);
            } else if (type == 'C') {
                handleCASRequest(txid, message, senderAddress, senderPort);
            }

        } catch (Exception e) {
            // Ignore bad messages.
        }
    }

    private void handleRelayRequest(
            String outerTxid,
            String message,
            java.net.InetAddress senderAddress,
            int senderPort
    ) {
        // Run relay work in a separate thread so the node does not block
        // while waiting for the relayed response.
        Thread relayThread = new Thread(() -> {
            try {
                // Expected format:
                // AA V <encoded target node name> <embedded full CRN message>
                //
                // Example:
                // AA V 0 N:test1 BB G

                if (message.length() < 6) {
                    return;
                }

                if (message.charAt(4) != ' ') {
                    return;
                }

                DecodedString decodedTarget = decodeString(message, 5);
                String targetNodeName = decodedTarget.value;

                if (targetNodeName == null || !targetNodeName.startsWith("N:")) {
                    return;
                }

                String embeddedMessage = message.substring(decodedTarget.nextIndex);

                if (embeddedMessage.length() < 4) {
                    return;
                }

                String targetAddressText = store.get(targetNodeName);

                if (targetAddressText == null) {
                    return;
                }

                NodeAddress target = parseNodeAddress(targetAddressText);

                // embeddedMessage is a full CRN message, e.g. "BB G".
                // sendRequest expects only the body, e.g. "G".
                // So remove the embedded txid and following space.
                if (embeddedMessage.charAt(2) != ' ') {
                    return;
                }

                String embeddedRequestBody = embeddedMessage.substring(3);

                String response = sendDirectRequest(embeddedRequestBody, target);

                if (response == null || response.length() < 4) {
                    return;
                }

                // Rewrite response transaction ID back to the outer relay txid.
                String relayResponse = outerTxid + response.substring(2);

                sendMessage(relayResponse, senderAddress, senderPort);

            } catch (Exception e) {
                // Ignore failed relay attempts.
            }
        });

        relayThread.start();
    }

    private void handleNameRequest(String txid, java.net.InetAddress senderAddress, int senderPort) throws Exception {
        String response = txid + " H " + encodeString(nodeName);
        sendMessage(response, senderAddress, senderPort);
    }

    private void sendMessage(String message, java.net.InetAddress address, int port) throws Exception {
        byte[] data = message.getBytes(java.nio.charset.StandardCharsets.UTF_8);

        java.net.DatagramPacket packet =
                new java.net.DatagramPacket(data, data.length, address, port);

        socket.send(packet);
    }

    private static class AddressPair {
        String key;
        String value;
        int distance;

        AddressPair(String key, String value, int distance) {
            this.key = key;
            this.value = value;
            this.distance = distance;
        }
    }

    private boolean isValidHashHex(String text) {
        if (text == null || text.length() != 64) {
            return false;
        }

        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);

            boolean isDigit = c >= '0' && c <= '9';
            boolean isLowerHex = c >= 'a' && c <= 'f';
            boolean isUpperHex = c >= 'A' && c <= 'F';

            if (!isDigit && !isLowerHex && !isUpperHex) {
                return false;
            }
        }

        return true;
    }

    private byte[] hexToBytes(String hex) throws Exception {
        if (!isValidHashHex(hex)) {
            throw new Exception("Invalid hashID");
        }

        byte[] bytes = new byte[32];

        for (int i = 0; i < 32; i++) {
            int index = i * 2;
            String byteText = hex.substring(index, index + 2);
            bytes[i] = (byte) Integer.parseInt(byteText, 16);
        }

        return bytes;
    }

    private int distanceFromKeyToHash(String key, String hashHex) throws Exception {
        byte[] keyHash = computeHash(key);
        byte[] targetHash = hexToBytes(hashHex);

        return distanceBetweenHashes(keyHash, targetHash);
    }

    private java.util.List<AddressPair> getClosestAddressPairs(String targetHashHex) throws Exception {
        java.util.List<AddressPair> pairs = new java.util.ArrayList<>();

        for (java.util.Map.Entry<String, String> entry : store.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();

            if (key != null && key.startsWith("N:")) {
                int d = distanceFromKeyToHash(key, targetHashHex);
                pairs.add(new AddressPair(key, value, d));
            }
        }

        pairs.sort((a, b) -> {
            if (a.distance != b.distance) {
                return Integer.compare(a.distance, b.distance);
            }

            return a.key.compareTo(b.key);
        });

        if (pairs.size() > 3) {
            return new java.util.ArrayList<>(pairs.subList(0, 3));
        }

        return pairs;
    }

    private void handleNearestRequest(
            String txid,
            String message,
            java.net.InetAddress senderAddress,
            int senderPort
    ) throws Exception {
        // Expected format: AA N <64-character-hashID>
        if (message.length() < 69) {
            return;
        }

        if (message.charAt(4) != ' ') {
            return;
        }

        String targetHashHex = message.substring(5).trim();

        if (!isValidHashHex(targetHashHex)) {
            return;
        }

        java.util.List<AddressPair> closest = getClosestAddressPairs(targetHashHex);

        StringBuilder response = new StringBuilder();
        response.append(txid).append(" O");

        for (AddressPair pair : closest) {
            response.append(" ");
            response.append(encodeString(pair.key));
            response.append(encodeString(pair.value));
        }

        sendMessage(response.toString(), senderAddress, senderPort);
    }

    private boolean isResponsibleForKey(String key) throws Exception {
        if (nodeName == null) {
            return false;
        }

        int myDistance = distance(nodeName, key);
        int strictlyCloserNodes = 0;

        for (String storedKey : store.keySet()) {
            if (storedKey == null || !storedKey.startsWith("N:")) {
                continue;
            }

            if (storedKey.equals(nodeName)) {
                continue;
            }

            int otherDistance = distance(storedKey, key);

            if (otherDistance < myDistance) {
                strictlyCloserNodes++;
            }
        }

        // If we know three strictly closer nodes, we are not responsible.
        return strictlyCloserNodes < 3;
    }

    private void handleExistsRequest(
            String txid,
            String message,
            java.net.InetAddress senderAddress,
            int senderPort
    ) throws Exception {
        // Expected format:
        // AA E <encoded key>
        //
        // Example:
        // AA E 0 D:Juliet-0

        if (message.length() < 6) {
            return;
        }

        if (message.charAt(4) != ' ') {
            return;
        }

        DecodedString decodedKey = decodeString(message, 5);
        String key = decodedKey.value;

        boolean hasKey = store.containsKey(key);
        boolean responsible = isResponsibleForKey(key);

        String code;

        if (hasKey) {
            code = "Y";
        } else if (responsible) {
            code = "N";
        } else {
            code = "?";
        }

        String response = txid + " F " + code + " ";
        sendMessage(response, senderAddress, senderPort);
    }

    private void handleReadRequest(
            String txid,
            String message,
            java.net.InetAddress senderAddress,
            int senderPort
    ) throws Exception {
        // Expected format:
        // AA R <encoded key>
        //
        // Example:
        // AA R 0 D:Juliet-0

        if (message.length() < 6) {
            return;
        }

        if (message.charAt(4) != ' ') {
            return;
        }

        DecodedString decodedKey = decodeString(message, 5);
        String key = decodedKey.value;

        boolean hasKey = store.containsKey(key);
        boolean responsible = isResponsibleForKey(key);

        String response;

        if (hasKey) {
            String value = store.get(key);
            response = txid + " S Y " + encodeString(value);
        } else if (responsible) {
            response = txid + " S N ";
        } else {
            response = txid + " S ? ";
        }

        sendMessage(response, senderAddress, senderPort);
    }

    private boolean isValidKey(String key) {
        return key != null && (key.startsWith("N:") || key.startsWith("D:"));
    }

    private void handleWriteRequest(
            String txid,
            String message,
            java.net.InetAddress senderAddress,
            int senderPort
    ) throws Exception {
        // Expected format:
        // AA W <encoded key> <encoded value>
        //
        // Example:
        // AA W 0 D:message 1 Hello World

        if (message.length() < 6) {
            return;
        }

        if (message.charAt(4) != ' ') {
            return;
        }

        DecodedString decodedKey = decodeString(message, 5);
        String key = decodedKey.value;

        DecodedString decodedValue = decodeString(message, decodedKey.nextIndex);
        String value = decodedValue.value;

        if (!isValidKey(key)) {
            String response = txid + " X X ";
            sendMessage(response, senderAddress, senderPort);
            return;
        }

        boolean alreadyHasKey = store.containsKey(key);
        boolean responsible = isResponsibleForKey(key);

        String code;

        if (alreadyHasKey) {
            storeKeyValue(key, value);
            code = "R"; // replaced
        } else if (responsible) {
            storeKeyValue(key, value);
            code = "A"; // added
        } else {
            code = "X"; // rejected
        }

        String response = txid + " X " + code + " ";
        sendMessage(response, senderAddress, senderPort);
    }

    private synchronized String generateTransactionID() {
        int base = TXID_CHARS.length();
        int value = nextTransactionNumber++;

        char first = TXID_CHARS.charAt((value / base) % base);
        char second = TXID_CHARS.charAt(value % base);

        return "" + first + second;
    }

    private static class NodeAddress {
        java.net.InetAddress address;
        int port;

        NodeAddress(java.net.InetAddress address, int port) {
            this.address = address;
            this.port = port;
        }
    }

    private NodeAddress parseNodeAddress(String addressText) throws Exception {
        if (addressText == null) {
            throw new Exception("Missing address");
        }

        int colonIndex = addressText.lastIndexOf(":");

        if (colonIndex <= 0 || colonIndex >= addressText.length() - 1) {
            throw new Exception("Bad address format");
        }

        String host = addressText.substring(0, colonIndex);
        int port = Integer.parseInt(addressText.substring(colonIndex + 1));

        return new NodeAddress(java.net.InetAddress.getByName(host), port);
    }

    private void discoverNodesForKey(String key) {
        try {
            String targetHash = computeHashHex(key);
            java.util.Set<String> queried = new java.util.HashSet<>();

            for (int round = 0; round < 4; round++) {
                java.util.List<AddressPair> closest = getClosestAddressPairsForKey(key);

                boolean learnedSomething = false;

                for (AddressPair pair : closest) {
                    try {
                        if (pair.key.equals(nodeName)) {
                            continue;
                        }

                        if (queried.contains(pair.key)) {
                            continue;
                        }

                        queried.add(pair.key);

                        NodeAddress target = parseNodeAddress(pair.value);

                        String requestBody = "N " + targetHash;

                        String response = sendRequest(requestBody, pair.key, target);

                        int learned = parseNearestResponse(response);

                        if (learned > 0) {
                            learnedSomething = true;
                        }

                    } catch (Exception e) {
                        // Try the next known node.
                    }
                }

                if (!learnedSomething) {
                    break;
                }
            }

        } catch (Exception e) {
            // Discovery is best-effort.
        }
    }

    private boolean isMatchingResponse(String message, String txid) {
        if (message == null || message.length() < 4) {
            return false;
        }

        if (!message.substring(0, 2).equals(txid)) {
            return false;
        }

        if (message.charAt(2) != ' ') {
            return false;
        }

        char type = message.charAt(3);

        return type == 'H' ||
                type == 'O' ||
                type == 'F' ||
                type == 'S' ||
                type == 'X' ||
                type == 'D';
    }


    public boolean isActive(String nodeName) throws Exception {
        if (nodeName == null) {
            return false;
        }

        String addressText = store.get(nodeName);

        if (addressText == null) {
            return false;
        }

        NodeAddress target = parseNodeAddress(addressText);

        String response = sendRequest("G", nodeName, target);

        if (response == null) {
            return false;
        }

        if (response.length() < 6 || response.charAt(3) != 'H') {
            return false;
        }

        DecodedString decodedName = decodeString(response, 5);

        return nodeName.equals(decodedName.value);
    }



    private String sendFullMessageReliably(String fullMessage, NodeAddress target) throws Exception {
        if (socket == null) {
            throw new Exception("Port has not been opened yet.");
        }

        if (fullMessage == null || fullMessage.length() < 4 || fullMessage.charAt(2) != ' ') {
            return null;
        }

        String txid = fullMessage.substring(0, 2);

        byte[] data = fullMessage.getBytes(java.nio.charset.StandardCharsets.UTF_8);

        java.net.DatagramPacket outgoingPacket =
                new java.net.DatagramPacket(data, data.length, target.address, target.port);

        // Initial send + up to 3 resends
        for (int attempt = 0; attempt <= 3; attempt++) {
            socket.send(outgoingPacket);

            long deadline = System.currentTimeMillis() + 5000;

            while (System.currentTimeMillis() < deadline) {
                long remaining = deadline - System.currentTimeMillis();

                if (remaining <= 0) {
                    break;
                }

                try {
                    socket.setSoTimeout((int) remaining);

                    byte[] buffer = new byte[4096];

                    java.net.DatagramPacket incomingPacket =
                            new java.net.DatagramPacket(buffer, buffer.length);

                    socket.receive(incomingPacket);

                    String incomingMessage = new String(
                            incomingPacket.getData(),
                            0,
                            incomingPacket.getLength(),
                            java.nio.charset.StandardCharsets.UTF_8
                    );

                    if (isMatchingResponse(incomingMessage, txid)) {
                        return incomingMessage;
                    }

                    // Not our response, so handle it normally.
                    handleSingleMessage(
                            incomingMessage,
                            incomingPacket.getAddress(),
                            incomingPacket.getPort()
                    );

                } catch (java.net.SocketTimeoutException e) {
                    break;
                } catch (Exception e) {
                    // Ignore malformed/unrelated packets.
                }
            }
        }

        return null;
    }

    private String sendDirectRequest(String requestBody, NodeAddress target) throws Exception {
        String txid = generateTransactionID();
        String fullMessage = txid + " " + requestBody;

        return sendFullMessageReliably(fullMessage, target);
    }

    private String sendRequest(String requestBody, String targetNodeName, NodeAddress target) throws Exception {
        // If no relays are active, just send directly.
        if (relayStack.isEmpty()) {
            return sendDirectRequest(requestBody, target);
        }

        // Build the original embedded request.
        String embeddedMessage = generateTransactionID() + " " + requestBody;

        String currentTargetName = targetNodeName;

        // Wrap from the end of the relay stack backwards.
        // Example relayStack: [N:relay1, N:relay2]
        // Final message to relay1 becomes:
        // AA V N:relay2 BB V N:target CC R key
        for (int i = relayStack.size() - 1; i >= 0; i--) {
            String relayName = relayStack.get(i);

            String outerTxid = generateTransactionID();
            embeddedMessage =
                    outerTxid + " V " + encodeString(currentTargetName) + embeddedMessage;

            currentTargetName = relayName;
        }

        // Send the outermost relay message to the first relay.
        String firstRelayName = relayStack.get(0);
        String firstRelayAddressText = store.get(firstRelayName);

        if (firstRelayAddressText == null) {
            return null;
        }

        NodeAddress firstRelayAddress = parseNodeAddress(firstRelayAddressText);

        return sendFullMessageReliably(embeddedMessage, firstRelayAddress);
    }

    private void handleCASRequest(
            String txid,
            String message,
            java.net.InetAddress senderAddress,
            int senderPort
    ) throws Exception {
        // Expected format:
        // AA C <encoded key> <encoded currentValue> <encoded newValue>
        //
        // Example:
        // AA C 0 D:score 0 10 0 11

        if (message.length() < 6) {
            return;
        }

        if (message.charAt(4) != ' ') {
            return;
        }

        DecodedString decodedKey = decodeString(message, 5);
        String key = decodedKey.value;

        DecodedString decodedCurrentValue = decodeString(message, decodedKey.nextIndex);
        String currentValue = decodedCurrentValue.value;

        DecodedString decodedNewValue = decodeString(message, decodedCurrentValue.nextIndex);
        String newValue = decodedNewValue.value;

        if (!isValidKey(key)) {
            String response = txid + " D X ";
            sendMessage(response, senderAddress, senderPort);
            return;
        }

        boolean alreadyHasKey = store.containsKey(key);
        boolean responsible = isResponsibleForKey(key);

        String code;

        synchronized (store) {
            if (alreadyHasKey) {
                String actualValue = store.get(key);

                if (actualValue != null && actualValue.equals(currentValue)) {
                    storeKeyValue(key, newValue);
                    code = "R"; // replaced
                } else {
                    code = "N"; // current value did not match
                }
            } else if (responsible) {
                storeKeyValue(key, newValue);
                code = "A"; // added
            } else {
                code = "X"; // rejected
            }
        }

        String response = txid + " D " + code + " ";
        sendMessage(response, senderAddress, senderPort);
    }


    public void pushRelay(String nodeName) throws Exception {
        if (nodeName == null || !nodeName.startsWith("N:")) {
            throw new Exception("Relay node name must start with N:");
        }

        relayStack.add(nodeName);
    }

    public void popRelay() throws Exception {
        if (!relayStack.isEmpty()) {
            relayStack.remove(relayStack.size() - 1);
        }
    }

    private java.util.List<AddressPair> getClosestAddressPairsForKey(String key) throws Exception {
        String hashHex = computeHashHex(key);
        return getClosestAddressPairs(hashHex);
    }

    public boolean exists(String key) throws Exception {
        if (!isValidKey(key)) {
            return false;
        }

        discoverNodesForKey(key);

        // Check local storage first.
        if (store.containsKey(key)) {
            return true;
        }

        java.util.List<AddressPair> closest = getClosestAddressPairsForKey(key);

        for (AddressPair pair : closest) {
            try {
                NodeAddress target = parseNodeAddress(pair.value);
                String requestBody = "E " + encodeString(key);

                String response = sendRequest(requestBody, pair.key, target);

                if (response == null) {
                    continue;
                }

                // Expected: AA F Y/N/?
                if (response.length() >= 6 && response.charAt(3) == 'F') {
                    char code = response.charAt(5);

                    if (code == 'Y') {
                        return true;
                    }

                    if (code == 'N') {
                        return false;
                    }
                }

            } catch (Exception e) {
                // Try the next closest node.
            }
        }

        return false;
    }

    public String read(String key) throws Exception {
        if (!isValidKey(key)) {
            return null;
        }

        discoverNodesForKey(key);

        // Check local storage first.
        if (store.containsKey(key)) {
            return store.get(key);
        }


        java.util.List<AddressPair> closest = getClosestAddressPairsForKey(key);

        for (AddressPair pair : closest) {
            try {
                NodeAddress target = parseNodeAddress(pair.value);
                String requestBody = "R " + encodeString(key);

                String response = sendRequest(requestBody, pair.key, target);

                if (response == null) {
                    continue;
                }

                // Expected:
                // AA S Y <encoded value>
                // AA S N
                // AA S ?
                if (response.length() >= 6 && response.charAt(3) == 'S') {
                    char code = response.charAt(5);

                    if (code == 'Y') {
                        DecodedString decodedValue = decodeString(response, 7);
                        return decodedValue.value;
                    }

                    if (code == 'N') {
                        return null;
                    }
                }

            } catch (Exception e) {
                // Try the next closest node.
            }
        }

        return null;
    }

    private void storeKeyValue(String key, String value) throws Exception {
        if (!isValidKey(key)) {
            return;
        }

        store.put(key, value);

        if (key.startsWith("N:")) {
            enforceAddressLimitForDistance(key);
            rebalanceData();
        }
    }

    private void enforceAddressLimitForDistance(String newAddressKey) throws Exception {
        if (nodeName == null || newAddressKey == null || !newAddressKey.startsWith("N:")) {
            return;
        }

        int targetDistance = distance(nodeName, newAddressKey);

        java.util.List<String> sameDistanceNodes = new java.util.ArrayList<>();

        for (String key : store.keySet()) {
            if (key != null && key.startsWith("N:")) {
                int d = distance(nodeName, key);

                if (d == targetDistance) {
                    sameDistanceNodes.add(key);
                }
            }
        }

        if (sameDistanceNodes.size() <= 3) {
            return;
        }

        // Prefer keeping ourselves if we are in this distance bucket.
        sameDistanceNodes.sort((a, b) -> {
            if (a.equals(nodeName)) {
                return -1;
            }

            if (b.equals(nodeName)) {
                return 1;
            }

            return a.compareTo(b);
        });

        while (sameDistanceNodes.size() > 3) {
            String removeKey = sameDistanceNodes.remove(sameDistanceNodes.size() - 1);

            if (!removeKey.equals(nodeName)) {
                store.remove(removeKey);
            }
        }
    }

    private void rebalanceData() {
        java.util.List<String> dataKeys = new java.util.ArrayList<>();

        for (String key : store.keySet()) {
            if (key != null && key.startsWith("D:")) {
                dataKeys.add(key);
            }
        }

        for (String dataKey : dataKeys) {
            try {
                if (isResponsibleForKey(dataKey)) {
                    continue;
                }

                String value = store.get(dataKey);

                if (value == null) {
                    continue;
                }

                java.util.List<AddressPair> closest = getClosestAddressPairsForKey(dataKey);

                boolean transferred = false;

                for (AddressPair pair : closest) {
                    if (pair.key.equals(nodeName)) {
                        continue;
                    }

                    NodeAddress target = parseNodeAddress(pair.value);

                    String requestBody =
                            "W " + encodeString(dataKey) + encodeString(value);

                    String response = sendDirectRequest(requestBody, target);

                    if (response != null &&
                            response.length() >= 6 &&
                            response.charAt(3) == 'X') {

                        char code = response.charAt(5);

                        if (code == 'A' || code == 'R') {
                            transferred = true;
                        }
                    }
                }

                if (transferred) {
                    store.remove(dataKey);
                }

            } catch (Exception e) {
                // Rebalancing is best-effort. Do not crash the node.
            }
        }
    }

    public boolean write(String key, String value) throws Exception {
        if (!isValidKey(key)) {
            return false;
        }

        discoverNodesForKey(key);
        boolean wroteSomewhere = false;

        java.util.List<AddressPair> closest = getClosestAddressPairsForKey(key);

        // Try to write to the closest known nodes.
        for (AddressPair pair : closest) {
            try {
                NodeAddress target = parseNodeAddress(pair.value);
                String requestBody = "W " + encodeString(key) + encodeString(value);

                String response = sendRequest(requestBody, pair.key, target);

                if (response == null) {
                    continue;
                }

                // Expected:
                // AA X A
                // AA X R
                // AA X X
                if (response.length() >= 6 && response.charAt(3) == 'X') {
                    char code = response.charAt(5);

                    if (code == 'A' || code == 'R') {
                        wroteSomewhere = true;
                    }
                }

            } catch (Exception e) {
                // Try the next closest node.
            }
        }

        // If we did not know any nodes, or all writes failed,
        // store locally if this node is responsible.
        if (!wroteSomewhere && isResponsibleForKey(key)) {
            storeKeyValue(key, value);
            wroteSomewhere = true;
        }

        return wroteSomewhere;
    }

    private int parseNearestResponse(String response) {
        try {
            if (response == null || response.length() < 4) {
                return 0;
            }

            if (response.charAt(3) != 'O') {
                return 0;
            }

            int index = 4;
            int learned = 0;

            while (index < response.length()) {
                if (response.charAt(index) == ' ') {
                    index++;
                }

                if (index >= response.length()) {
                    break;
                }

                DecodedString decodedKey = decodeString(response, index);
                DecodedString decodedValue = decodeString(response, decodedKey.nextIndex);

                String key = decodedKey.value;
                String value = decodedValue.value;

                if (key != null && key.startsWith("N:")) {
                    storeKeyValue(key, value);
                    learned++;
                }

                index = decodedValue.nextIndex;
            }

            return learned;

        } catch (Exception e) {
            return 0;
        }
    }

    public boolean CAS(String key, String currentValue, String newValue) throws Exception {
        if (!isValidKey(key)) {
            return false;
        }

        discoverNodesForKey(key);

        java.util.List<AddressPair> closest = getClosestAddressPairsForKey(key);

        for (AddressPair pair : closest) {
            try {
                NodeAddress target = parseNodeAddress(pair.value);

                String requestBody =
                        "C " +
                                encodeString(key) +
                                encodeString(currentValue) +
                                encodeString(newValue);

                String response = sendRequest(requestBody, pair.key, target);

                if (response == null) {
                    continue;
                }

                // Expected:
                // AA D R
                // AA D N
                // AA D A
                // AA D X
                if (response.length() >= 6 && response.charAt(3) == 'D') {
                    char code = response.charAt(5);

                    if (code == 'R' || code == 'A') {
                        return true;
                    }

                    if (code == 'N') {
                        return false;
                    }
                }

            } catch (Exception e) {
                // Try next closest node.
            }
        }

        // Local fallback if no network node handled it.
        synchronized (store) {
            if (store.containsKey(key)) {
                String actualValue = store.get(key);

                if (actualValue != null && actualValue.equals(currentValue)) {
                    storeKeyValue(key, newValue);
                    return true;
                }

                return false;
            }

            if (isResponsibleForKey(key)) {
                storeKeyValue(key, newValue);
                return true;
            }
        }

        return false;
    }
}
