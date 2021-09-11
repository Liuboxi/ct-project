package ct.common.constant;

import ct.common.bean.Val;

public enum Names implements Val {
    NAMESPNAMES("ct"), TOPIC("ct"),
    CF_INFO("info"),TABLE("ct:calllog"),
    CF_CALLER("caller"),CF_CALLEE("callee");

    private String name;

    private Names(String name) {
        this.name = name;
    }

    @Override
    public void setValue(Object val) {
        this.name = (String) val;
    }

    @Override
    public String getValue() {
        return name;
    }
}
