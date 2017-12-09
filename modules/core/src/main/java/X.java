/**
 * <p> The <code>X</code> </p>
 *
 * @author Alexei Scherbakov
 */
public class X {
    private static class Base {
        public void print() {
            System.out.println(this);
        }
    }

    private static class A1 extends Base {
        public void print() {
            super.print();

            System.out.println(this);
        }
    }

    public static void main(String[] args) {
        new A1().print();
    }
}
