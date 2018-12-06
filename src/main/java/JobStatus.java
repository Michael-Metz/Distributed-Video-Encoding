import com.google.gson.Gson;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@NoArgsConstructor
@RequiredArgsConstructor
public @Data
class JobStatus {
    private Job job;
    @NonNull
    private boolean complete;
    @NonNull
    private String workerSignature;

    public String toJson() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }

}
