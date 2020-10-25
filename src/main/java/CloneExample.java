import java.io.Serializable;

public class CloneExample implements Cloneable, Serializable {
    @Override
    protected Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
}
