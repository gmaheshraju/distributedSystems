package com.concepts.replicated.state.machine;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

class ReplicatedStateMachine {
    private final ConcurrentHashMap<String, String> state = new ConcurrentHashMap<>();

    // Apply a command to the state machine
    public synchronized void applyCommand(String command) {
        String[] parts = command.split(" ");
        String action = parts[0];
        String key = parts[1];

        switch (action) {
            case "PUT":
                String value = parts[2];
                state.put(key, value);
                System.out.println("PUT: Key=" + key + ", Value=" + value);
                break;
            case "DELETE":
                state.remove(key);
                System.out.println("DELETE: Key=" + key);
                break;
            default:
                throw new IllegalArgumentException("Unknown command: " + action);
        }
    }

    public String getValue(String key) {
        return state.get(key);
    }
}

class ConsensusModule {
    private final LinkedBlockingQueue<String> commandQueue = new LinkedBlockingQueue<>();
    private final ReplicatedStateMachine stateMachine;

    public ConsensusModule(ReplicatedStateMachine stateMachine) {
        this.stateMachine = stateMachine;
        startConsensusThread();
    }

    // Simulate a simple consensus mechanism
    public void proposeCommand(String command) throws InterruptedException {
        System.out.println("Proposing command: " + command);
        commandQueue.put(command);
    }

    private void startConsensusThread() {
        Thread consensusThread = new Thread(() -> {
            while (true) {
                try {
                    String command = commandQueue.take();
                    System.out.println("Applying command: " + command);
                    stateMachine.applyCommand(command);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
        consensusThread.start();
    }
}

public class ReplicatedStateMachineExample {
    public static void main(String[] args) throws InterruptedException {
        ReplicatedStateMachine stateMachine = new ReplicatedStateMachine();
        ConsensusModule consensusModule = new ConsensusModule(stateMachine);

        // Simulate clients sending commands
        consensusModule.proposeCommand("PUT key1 value1");
        consensusModule.proposeCommand("PUT key2 value2");
        consensusModule.proposeCommand("DELETE key1");

        // Allow time for commands to be processed
        Thread.sleep(1000);

        // Verify the state
        System.out.println("Final state of key2: " + stateMachine.getValue("key2"));
    }
}
