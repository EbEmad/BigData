public class Admin extends User {

    public Admin(String name) {
        super(name);
    }

    @Override
    public boolean isAdmin() {
        return true;
    }
}