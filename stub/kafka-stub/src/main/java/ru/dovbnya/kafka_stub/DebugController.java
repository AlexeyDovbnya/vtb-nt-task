package ru.dovbnya.kafka_stub;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

/** Только для проверки задания 6: забивает кучу до OutOfMemoryError */
@RestController
public class DebugController {

    private static final List<byte[]> LEAK = new ArrayList<>();

    @PostMapping("/debug/oom")
    public String oom() {
        while (true) {
            LEAK.add(new byte[10 * 1024 * 1024]);   // по 10 МБ, пока не кончится куча
        }
    }
}