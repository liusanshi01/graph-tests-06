package xyz.macor.graph.data;

public enum DataType {
    Node("node",1),
    Edge("edge",2);
    private String typeName;
    private int typeCode;

    private DataType(String datatype,int ind){
        this.typeName = datatype;
        this.typeCode = ind;
    }

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    public int getTypeCode() {
        return typeCode;
    }

    public void setTypeCode(int typeCode) {
        this.typeCode = typeCode;
    }
}
