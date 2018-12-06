
import com.google.gson.Gson;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@EqualsAndHashCode(callSuper = false)
@NoArgsConstructor
public class Job implements Serializable {

    private long id;
    private long videoid;
    private long uploadid;
    private String oldvideofilename;
    private String newvideofilename;
    private int width;
    private int height;
    private int crf;
    private int audiobitrate;
    private boolean inprogress;
    private boolean complete;

    public String toJson() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }
}
