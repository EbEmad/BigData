public class User {
    private String name;

    public User(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public boolean isAdmin() {
        return false;
    }
}