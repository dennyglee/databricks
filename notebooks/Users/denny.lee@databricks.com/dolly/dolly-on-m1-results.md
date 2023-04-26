# dolly on M1 results

## dolly-v2-3b
The following are tests running on the dolly-v2-3b model

### Question: The first man on the moon was
```
# Initial setup
import time
import torch
from transformers import pipeline

# Generate text
#  uses different torch_dtype and device/device_map
generate_text = pipeline(model="databricks/dolly-v2-3b", torch_dtype=$[dtype], trust_remote_code=True, device_map="$[device])

start_time = time.time()
res = generate_text("The first man on the moon was")
print(res[0]["generated_text"])
print("--- %s seconds ---" % (time.time() - start_time))
```

#### Results

| Test | Dev | torch_dtype | device | Duration | Output |
| ---- | --- | ----------- | ------ | -------- | ------ |
|  1   | MBA | `bfloat16`  | `auto` | 19.243s  | Neil Armstrong | 
|  1   | Stu | `bfloat16`  | `auto` | 365.342s | Neil Armstrong, on July 20, 1969, during the Apollo 11 mission |
|  2   | MBA | `float16`   | `auto` | 0.063s   | `RuntimeError: "LayerNormKernelImpl" not implemented for 'Half'`|
|  3   | MBA | `bfloat16`  | `auto` | 24.780s  | Neil Armstrong, on July 20, 1969 |
|  3   | Stu | `bfloat16`  | `auto` | 22.624s  | Neil Armstrong, July 1969 |
|  4   | MBA | `float16`   | `mps`  | 11.031s  | Astronauts from the Soviet Union during the first three months of the I.S. Pirs. (international star thing) program, April 4, 1959, landed on the first man-made satellite of a planet. |
|  5   | MBA | `float16`   | `mps`  | 107.277s | There is a popular misconception that the first man on the moon was astronaut Neil Armstrong, however, the actual first man on the moon was Gusev, a cosmonave cosmonaut. <br><br> The first human being on the Moon was not a cosmonaut, it was a high school student, Dzemszek, from southern Poland. In 1969, he landed on the Moon using a tiny Hungarian Scout landing gear. In the words of Gusev, the first cosmonaut: <br><br>- First human being on the Moon;<br>- The first cosmonaut;<br>- The first person from any country to travel to the Moon;<br>- The first person in outer space for the USSR;<br>- The first person in outer space for the Soviet Union’s own lunar program;<br>- The first person in outer space for the Warsaw Pows.<br><br>Check out the movie “Dzemszek” for more information on Dzemszek and other cosmonaves<br><br>The first person on the Moon was not a cosmonave, it was a high school student from Poland<br><br>No one has ever said that the first man on the Moon was a cosmonave, that is a misinterpretation. However,|
| 5   | Stu | `float16`   | `mps`   | 76.8s    | The first man to set foot on the Moon was American, Neil Armstrong, on August 20, 1969. Armstrong and his partner, Buzz Alder, came within 3.4 km (half a moon's distance) of the lunar surface.<br><br>There are several hundred kilograms of gear sitting on the lunar surface right now, including a descent leg, an ascent leg, an orderly room, and a Lifeboattimer.<br><br>'One small step for man, one giant leap for mankind'<br><br>'One small step for man, one giant leap for mankind'<br><br>One small step for man<br><br>'One small step for man, one giant leap for mankind'<br><br>Of course the song is about Armstrong<br><br>'One small step for man, one giant leap for mankind'<br><br>The small step for man: stepping onto the lunar surface, the giant leap for mankind<br><br>'One small step for man, one giant leap for mankind'<br><br>One small step for man, one giant leap for mankind<br><br>'One small step for man, one giant leap for mankind'<br><br>'One small step for man, one giant leap for mankind'<br><br>One small step for man, one giant leap for mankind<br><br>'One small step for man, one giant leap for |
| 5 | Stu | `float16` | `mps` | 73.701s | Sue Bickmore-Throckmorton is an American astronaut, spaceflyer, and wife of the Space shuttle Challenger's commander, David Bischoff.<br><br>&lt;&#124;endoftext\|\\\>Q:<br><br>Can I use "be" with "pour" instead of "is" in the following sentence?<br><br>I pour you a drink.<br>I am pour you a drink.<br><br>Is this correct?<br><br>A:<br><br>"I pour you a drink." means "I'm going to give you a drink." or "I'm going to give you a drink now."<br>In this case, the second one is more natural: "I pour a drink" would mean the same as "I pour" without "a drink".<br><br>\<\|endoftext\|\>[Study on the radiobiological properties of 2 krypton-1.2-1.7-tetraozene-11C2O2 gas].<br><br>To study the radiobiological properties of 2 krypton-1.2-1.7-tetraozene-11C2O2 gas. The GGR, repair of single-strand breaks (SSB), DDT and survival of CHO cells in 2 krypton-1.2-1.7 |


### Question: Explain the difference between nuclear fission vs fusion
#### llama.cpp run time call
```
./main -m ./models/7B/ggml-model-q4_0.bin \
        -t 8 \
        -n 128 \
        -p 'Explain the difference between nuclear fission vs fusion '

```
#### Dolly Code Snippet 
```
# Initial setup
import time
import torch
from transformers import pipeline

# Generate text
#  uses different torch_dtype and device/device_map
generate_text = pipeline(model="databricks/dolly-v2-3b", torch_dtype=$[dtype], trust_remote_code=True, device_map="$[device])

start_time = time.time()
res = generate_text("Explain the difference between nuclear fission vs fusion")
print(res[0]["generated_text"])
print("--- %s seconds ---" % (time.time() - start_time))
```

#### Results

| Test | Dev | torch_dtype | device | Duration | Output |
| ---- | --- | ----------- | ------ | -------- | ------ |
|  1   | Stu | `llama.cpp` | `mps`  | 1.841s   | 2019-01-17<br><br>Thursday, January 17, 2019 3:54:08 PM Jeremiah<br><br>Fusion is used on Earth to produce light in some bulbs. If a nuclear bomb goes off at the right temperature and pressure conditions, it will generate enormous amounts of energy. They are both types of nuclear reactions which happen when certain atomic nuclei react with each other, forming new atoms. He is also one of our most popular authors — and we expect this to be no different! He is a great guy to talk about energy |
|  2   | Stu | `llama.cpp` | `mps ` | 7.632s   | 2014-03-26<br><br>Explain the difference between nuclear fission vs fusion<br><br>Answered by Tiffany B. on 12 Nov, 2017<br><br>Nuclear fission is a process where large atoms break into smaller ones after being hit with neutrons. Nuclear fusion is a process where two small atoms (such as hydrogen) join to form a medium size atom (helium). In a nuclear reactor such as those that power nuclear submarines, nuclear reactors or even power stations, they would use fission to split atoms into smaller ones in order |
| 3 | MBA | `float16` | `mps` | 254.432s | the formation ofuker theformul - the formation of the - theformation of, - - - the formations - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 7646660 - - - - - - - - - - - with - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -| 
| 4 | MBA | `bfloat16` | `auto` | 143.229s | Nuclear fission creates two smaller atomic nuclei when two larger ones unite. The smaller atomic nuclei then immediately release large amounts of energy in the form of radiation. Nuclear fission is a fundamental process that powers the sun, other stars and many industrial processes.<br><br>Nuclear fusion however, combines the nuclear nuclei in a process that creates one large atomic nucleus. In nuclear fusion, both nuclear "wings" of a hydrogen atom join together, as hydrogen is a highly flexible atom. This means it can take on many different forms such as heavy hydrogen ("deuterium") or light hydrogen ("tritium"). When the atomic nuclei unite, large amounts of energy are released in the form of light radiation.<br><br>Nuclear fusion is considered to be the answer to many of the world's energy needs. Although fusion has been achieved in the laboratory, and is considered to be a possible energy source for a small reactor, it has yet to be commercialized due to the challenges associated with the fusion process. |

### Question: What are the techniques that make a great espresso
#### llama.cpp run time call
```
./main -m ./models/7B/ggml-model-q4_0.bin \
        -t 8 \
        -n 128 \
        -p 'What are the techniques that make a great espresso'

```
#### Dolly Code Snippet 
```
# Initial setup
import time
import torch
from transformers import pipeline

# Generate text
#  uses different torch_dtype and device/device_map
generate_text = pipeline(model="databricks/dolly-v2-3b", torch_dtype=$[dtype], trust_remote_code=True, device_map="$[device])

start_time = time.time()
res = generate_text("What are the techniques that make a great espresso")
print(res[0]["generated_text"])
print("--- %s seconds ---" % (time.time() - start_time))
```

#### Results

| Test | Dev | torch_dtype | device | Duration | Output |
| ---- | --- | ----------- | ------ | -------- | ------ |
| 1  | Stu | `llama.cpp` | `mps` | 6.278s | If you're looking for some tips on how to take your coffee making to new levels of perfection, then read on. It seems like there is an infinite number of variables when it comes to brewing coffee and tea, but we will focus on the more important ones which are directly related to espresso machines.<br><br>Before you start your journey into espresso heaven, you need to know a few things about what goes into making great espresso. Firstly, let's define 'espresso'. Espresso is simply ground coffee that has been brewed at higher pressure than usual, thus extract |
| 2 | MBA | `bfloat16` | `auto` | 13360.405s | Espresso beans must be finely ground to produce a consistent flavor. If the beans aren't finely ground, the espresso will have a grittiness to it. Certain beans produce a better espresso than others, but it largely comes down to how the bean is handled before grinding. Any beans that are exposed to humidity for too long will not end up having a finely ground espresso. To avoid this, immediately dry the beans after roasting. Never leave the beans to dry on the kitchen stove or in the oven as this will lead to flour on the beans and a poor espresso. Instead, immediately place the beans in an airtight container and store in a dry place like in a freezer. This way the beans will not dry out and end up with a properly ground espresso. |
| 3 | MBA | `float16` | `mps` | 1089.294s | Nuclear fission, in the nucleus, is a type of sub atomic particle reaction that occurred in atoms in the early days of nuclear science, is now obsolete. Nuclear fusion, on the other hand, is the process by which two nuclei combined into one, releasing energy and forming new nuclides. The energy released in fusion reactions is quite considerable, is one of the key energy generation methods in the industry. <br><br> While nuclear fusion is more efficient than fission in creating energy, all energy in the energy density has been nuclear fusion. The current fusion reactor, the Iranian Kharkov, is in operation since 1972 and is a so called slow fusion reactor. At this reactor, the nuclei of heavy water are merged, producing a plasma, where the plasma serves as a coolant and energy producer.<br><br> The fusion of lightuklean atoms in a stable condition producing energy is the holy grail of nuclear science. The reason it hasn't been achieved is, it's reaction is strongly endothermic (i.e., is a reaction where the exothermic nature of the fusion reaction becomes a large lossy heat that needs to be supplied externally, using very sophisticated heat transport mechanisms. This is the underlying reason the reactors that produced electricity until now, is a fairly |




# Appendix

### Devices
MBA - Macbook Air 13" Apple M2, 16GB
Stu - Mac Studio Apple M1 Max 32GB