import java.util.ArrayList;

public class ChatSystem {
    private ArrayList<User> users = new ArrayList<>();
    private ArrayList<Message> messages = new ArrayList<>();

    public void addUser(User user) {
        users.add(user);
    }

    public ArrayList<User> getUsers() {
        return users;
    }

    public void sendMessage(User sender, User receiver, String content) {
        messages.add(new Message(sender, receiver, content));
    }

    public void broadcast(User sender, String content) {
        if (!sender.isAdmin()) {
            System.out.println("Only admin can broadcast!");
            return;
        }

        for (User user : users) {
            if (user != sender) {
                messages.add(new Message(sender, user, content));
            }
        }
    }

    public void viewMessages(User user) {
        System.out.println(user.getName() + " received:");

        for (Message m : messages) {
            if (m.getReceiver() == user) {
                System.out.println("From " + m.getSender().getName() + ": " + m.getContent());
            }
        }
    }

    public User findUserByName(String name) {
        for (User user : users) {
            if (user.getName().equalsIgnoreCase(name)) {
                return user;
            }
        }
        return null;
    }
}