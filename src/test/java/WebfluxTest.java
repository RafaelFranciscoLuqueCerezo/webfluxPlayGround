import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.List;

@ExtendWith(MockitoExtension.class)
class WebfluxTest {
    /********************************************************
     ** METODOS PARA EL PLAYGROUND Y TEST CASES, NO TOCAR!***
     ********************************************************/
    private Mono<Integer> monoInteger(int number) {
        System.out.println(String.format("monoInteger %d", number));
        return this.getMonoInteger(number);
    }

    private Mono<Void> monoEmpty(int number) {
        System.out.println(String.format("monoEmpty %d", number));
        return this.getMonoInteger(number).then();
    }

    private Mono<Integer> monoIntegerOne() {
        System.out.println("monoIntegerOne");
        return this.getMonoInteger(1);
    }

    private Mono<Integer> monoIntegerTwo() {
        System.out.println("monoIntegerTwo");
        return this.getMonoInteger(2);
    }

    private Mono<Integer> monoIntegerThree() {
        System.out.println("monoIntegerThree");
        return this.getMonoInteger(3);
    }

    private Mono<Integer> monoIntegerExceptionSaveETAG() {
        System.out.println("monoIntegerException");
        return monoIntegerOne().flatMap(response -> {
            return monoIntegerException();
        }).retryWhen(Retry.fixedDelay(3, Duration.ofSeconds(2)));

    }

    private Mono<Integer> monoIntegerException() {
        System.out.println("monoIntegerException");
        return Mono.defer(() -> {
            System.out.println("monoIntegerException inside....");
            throw new RuntimeException("exception");
        });
    }

    //LLamada asincrona a base de datos
    private Mono<Integer> getMonoInteger(Integer integer) {
        return Mono.just("searching...")
                .map(r -> {
                    System.out.println("searching[" + integer + "]");
                    try {
                        Thread.sleep(4000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    System.out.println("completed[" + integer + "]");
                    /*for(int i=0;i<10000;i++){
                        System.out.println(integer);
                    }*/
                    return integer;
                });
    }

    /************************
     *** MANEJO DE ERRORES **
     ************************/
    @Test
    void errorsDifferentTypes() {
        /*
         * Si no manejamos el error, cuando llegue al elemento 2, se corta la ejecucion,
         *  los elementos del 3-6 no se ejecutan y se corta con una excepcion el flujo
         */
        Flux.range(1, 6)
                .flatMap(element -> {
                    if (element == 2) {
                        return monoIntegerException();
                    }
                    return monoInteger(element);
                })
                .collectList()
                .subscribe();
    }

    @Test
    void onErrorResume() {
        /*
         * onErrorResume
         * return fallback value for entire stream (mono/flux).
         * E.g. if there’s a flux of 6 elements, and error happens on element 2, then rest 3,4,5… won’t be executed,
         * instead the fallback value will be considered.
         */
        Flux.range(1, 6)
                .flatMap(element -> {
                    if (element == 2) {
                        return monoIntegerException();
                    }
                    return monoInteger(element);
                })
                .onErrorResume(error -> {
                    return Mono.just(0);
                })
                .collectList()
                .subscribe(result -> {
                    System.out.println(result);
                });
    }

    @Test
    void onErrorContinue() {
        /*
         * onErrorContinue
         * consumes (error,data) and does NOT split it over. It considers the consumer for the error elements,
         * and leave the downstream chain as it for good elements. E.g. if there’s a flux of 6 elements, and error happens on element 2,
         * then all elements (1 to 6) except 2 will have normal execution,
         * but element 2 will have a different execution as mentioned in the consumer of onErrorContinue
         */
        Flux.range(1, 6)
                .flatMap(element -> {
                    if (element == 2) {
                        return monoIntegerException();
                    }
                    return monoInteger(element);
                })
                .onErrorContinue((error, data) -> {
                    System.out.println(data);
                })
                .collectList()
                .subscribe(result -> {
                    System.out.println(result);
                });
    }

    @Test
    void onErrorReturn() {
        /*
         * return fallback value for entire stream (mono/flux). E.g. if there’s a flux of 6 elements,
         * and error happens on element 2, then rest 3,4,5,6 won’t be executed,
         * instead the fallback value will be considered.
         */
        Flux.range(1, 6)
                .flatMap(element -> {
                    if (element == 2) {
                        return monoIntegerException();
                    }
                    return monoInteger(element);
                })
                .onErrorReturn(Exception.class, -1)
                .collectList()
                .subscribe(result -> {
                    System.out.println(result);
                });
    }

    @Test
    void doOnError() {
        /*
         * consume el error y corta la ejecucion para los siguientes elementos del stream.
         */
        Flux.range(1, 6)
                .flatMap(element -> {
                    if (element == 2) {
                        return monoIntegerException();
                    }
                    return monoInteger(element);
                })
                .doOnError(throwable -> {
                    System.out.println("something wrong happens....");
                })
                .collectList()
                .subscribe(result -> {
                    System.out.println(result);
                });
    }

    @Test
    void onErrorMap() {
        /*
         * castea un error en otro, PARA la ejecucion en los siguientes elementos del stream
         */
        Flux.range(1, 6)
                .flatMap(element -> {
                    if (element == 2) {
                        return monoIntegerException();
                    }
                    return monoInteger(element);
                })
                .onErrorMap(throwable -> {
                    return new NoSuchMethodError();
                })
                .collectList()
                .subscribe(result -> {
                    System.out.println(result);
                });
    }

    /****************************
     ***** CONCEPTOS BASICOS ****
     ****************************/
    @Test
    void callMonoWithoutSubscription() {
        /*
         * Al llamar a un metodo Mono SIN SUBSCRIBIRNOS A EL, webflux, prepara por asi
         * decirlo, el flujo, pero no llega a meterse dentro del metodo, vemos que solo se printea
         * la primera parte de la informacion, el system.out.println que esta dentro del metodo al que estamos
         * llamando. PERO ! no se esta printeando la simulacion de la llamada a base de datos, informacion
         * que podemos encontrar dentro del metodo getMonoInteger() el cual devuelve otro mono.
         * POR TANTO, podemos concluir, que llamar a un metodo mono sin suscribirnos, garantiza que realizara las operaciones pertinentes
         * hasta encontrar otro metodo mono que tenga que llamar internamente.
         */
        monoInteger(2);
    }

    @Test
    void callMonoWithSubscription() {
        /*
         * Al llamar a un metodo Mono y suscribirnos a el,
         * se realizara el flujo completo, llamando a tantos monos internos como necesite
         */
        monoInteger(2).subscribe();
    }

    @Test
    void callMonoWithFatherSubscription() {
        /*
         * Al suscribirnos en el flujo padre,
         * todos los flujos hijos van a realizar su flujo completo.
         * IMPORTANTE, no tenemos que poner .subscribe() a todos los metodos, basta con ponerlo en el flujo padre
         */
        monoIntegerOne()
                .flatMap(response -> monoIntegerTwo())
                .flatMap(response2 -> monoIntegerThree())
                .subscribe();
    }

    @Test
    void callMonosWithZip() {
        /*
         * Sirve para combinar monos que esperan una respuesta.
         * En este caso, llamamos a monoInteger(1) primero, obtenemos respuestsa, luego a monoIntenger(2) obtenemos respuesta
         * y por ultimo, cuando tenemos la respuesta de AMBOS Monos, retornamos una llamada a monoInteger(100)
         */
        var mono1 = monoInteger(1);
        var mono2 = monoInteger(2);
        Mono.zip(mono1, mono2).flatMap(tuple -> {
            System.out.println(tuple.getT1());
            System.out.println(tuple.getT2());
            return monoInteger(100);
        }).subscribe();
    }

    @Test
    void callMonoWithZipWhenException() {
        /*
         * IMPORTANTE! para continuar con el flujo, necesitamos que SI o SI, todos los monos implicados en el Mono.zip
         * devuelvan un resultado, si hay excepcion, o devuelven un Mono.empty, NO se seguira el flujo en el Mono.zip
         */
        var mono1 = monoIntegerException();
        var mono2 = monoInteger(2);
        Mono.zip(mono1, mono2).flatMap(tuple -> {
            System.out.println(tuple.getT1());
            System.out.println(tuple.getT2());
            return monoInteger(100);
        }).subscribe();
    }

    @Test
    void callMonoWithZipWhenReturnEmpty() {
        /*
         * IMPORTANTE! para continuar con el flujo, necesitamos que SI o SI, todos los monos implicados en el Mono.zip
         * devuelvan un resultado, si hay excepcion, o devuelven un Mono.empty, NO se seguira el flujo en el Mono.zip
         * IMPORTANTE! Mono.zip ejecuta en orden los monos que le pasas por parametros. Por ejemplo: si mono1 retorna error, mono2 no se ejecutara
         * PERO si mono1 esta okey, se ejecuta y si luego mono2 arroja exepcion, no sigue el flujo dentro del Mono.zip, pero mono1, habra sido completado exitosamente
         * En este caso mono1, retorna Void, por tanto, mono1 se va a ejecutar correctamente, ya que no hay ningun error
         * PERO nunca se ejecuta mono2, ya que mono1 no ha devuelto un resultado, ha devuelto Void
         */
        var mono1 = monoEmpty(1);
        var mono2 = monoInteger(2);
        Mono.zip(mono1, mono2).flatMap(tuple -> {
            System.out.println(tuple.getT1());
            System.out.println(tuple.getT2());
            return monoInteger(100);
        }).subscribe();
        /*
         * Invertimos el orden para comprobar que mono4 se ejecuta correctamente, mirar logs de consola del tests,
         * mientras que no seguimos el flujo que hay dentro del Mono.zip
         */
        var mono3 = monoEmpty(3);
        var mono4 = monoInteger(4);
        Mono.zip(mono4, mono3).flatMap(tuple -> {
            System.out.println(tuple.getT1());
            System.out.println(tuple.getT2());
            return monoInteger(100);
        }).subscribe();
    }


    @Test
    void parallelMonoSubscribeOnZip() {
        /*
         * con subscribeOn, se subscriben en los threads que quieras, todos los flujos que hay arriba y debajo,
         *  sin importar el orden del flujo de webflux
         * por ejemplo , en mono1 seria :
         *  PARALELO  -> monoInteger(1)
         *  PARALELO  -> monoInteger(11)
         *  PARALELO  -> monoInteger(111)
         */
        var mono1 = monoInteger(1)
                .subscribeOn(Schedulers.parallel())
                .flatMap(number -> monoInteger(11))
                .flatMap(number -> monoInteger(111));
        var mono2 = monoInteger(2)
                .subscribeOn(Schedulers.parallel())
                .flatMap(number -> monoInteger(22))
                .flatMap(number -> monoInteger(222));
        var finalMono = Mono.zip(mono1, mono2).doOnNext(tuple -> {
            System.out.println(tuple.getT1());
            System.out.println(tuple.getT2());
        }).then();
        StepVerifier.create(finalMono).verifyComplete();
    }

    @Test
    void parallelMonoPublishOnZip() {
        /*
         * con publishOn, se publican en los threads que quieras, desde ese punto hacia abajo , en el flujo de webflux
         * por ejemplo , en mono1 seria :
         *  SECUENCIAL-> monoInteger(1)
         *  PARALELO  -> monoInteger(11)
         *  PARALELO  -> monoInteger(111)
         */
        var mono1 = monoInteger(1)
                .publishOn(Schedulers.parallel())
                .flatMap(number -> monoInteger(11))
                .flatMap(number -> monoInteger(111));
        var mono2 = monoInteger(2)
                .publishOn(Schedulers.parallel())
                .flatMap(number -> monoInteger(22))
                .flatMap(number -> monoInteger(222));
        var finalMono = Mono.zip(mono1, mono2).doOnNext(tuple -> {
            System.out.println(tuple.getT1());
            System.out.println(tuple.getT2());
        }).then();
        StepVerifier.create(finalMono).verifyComplete();
    }

    @Test
    void parallelOnFlux() {
        /*
         * tenemos que poner parallel para indicar que es un parallelFlux
         * ruOn para indicar en que hilos queremos que corra
         * Lo que hay desde runOn hasta sequential, se hace de forma paralela
         * El metodo sequential, hace cuello de botella y desde ese punto en adelante la ejecucion se vuelve secuencial
         */
        var mono = Flux.range(1, 6)
                .parallel()
                .runOn(Schedulers.parallel())
                .flatMap(number -> monoInteger(number))
                .doOnNext(value -> System.out.println(String.format("FINISHED %d", value)))
                .sequential()
                .collectList()
                .then();
        StepVerifier.create(mono).verifyComplete();
    }

    @Test
    void thenOperator() {
        /*
         * Podemos comprobar que con el operador then si que entramos antes de terminar la ejecucion anterior, PERO
         * ATENCION a lo que sale por consola, nos quedamos solo hasta que tengamos que volver a interactuar con otro mono.
         * Cuando tengamos que interactuar con otro mono dentro de la funcion que hemos puesto en el .then(xxxx), se quedara a la espera
         * a que termine el flujo padre
         */
        monoIntegerOne().then(monoIntegerTwo().then(monoIntegerThree())).subscribe();
    }

    @Test
    void thenOperatorWithMonoDefer() {
        /*
         * Con Mono.defer, estamos diciendole que solo se llame cuando se haya completado el flujo padre, y  ... NI TAN SIQUIERA
         * prepara cosas hasta llegar a un Mono, NO SE LLAMA HASTA QUE ACABE EL PADRE
         */
        monoIntegerOne().then(Mono.defer(this::monoIntegerTwo).then(Mono.defer(this::monoIntegerThree))).subscribe();
    }


    /****************************
     ***** ZONA DE PRUEBAS   ****
     ****************************/
    /*
     * Prueba los conceptos que tengas en mente y
     * comprueba el comportamiento de webflux
     */
    @Test
    void erroresmanejo() {
        monoIntegerException()
                .then(monoIntegerTwo())
                .subscribe();
    }


    @Test
    void erroresmanejoFlux() {
        Flux.range(0, 3)
                .flatMap(value -> {
                    if (value == 1) {
                        return monoIntegerException()
                                .onErrorContinue((error, data) -> {
                                    System.out.println("continue on error");
                                });
                    }
                    return monoInteger(value);
                })
                .doOnNext(next -> {
                    System.out.printf("next for <%s> is okey%n", next);
                })
                .onErrorContinue((error, data) -> {
                    System.out.printf("error for <%s>%n", data);
                })
                .collectList()
                .subscribe();

    }


    @Test
    void testingNULL() {
        monoIntegerOne().flatMap(response -> Mono.empty()).flatMap(response2 -> monoInteger(99)).subscribe();
    }

    @Test
    void delay() {
        var mono = monoIntegerTwo().delayElement(Duration.ofSeconds(6)).doOnNext(response ->
                System.out.println(String.format("estas dentro %d", response)));
        StepVerifier.create(mono).verifyComplete();
    }

    @Test
    void errorsOperator() {
        monoIntegerOne().flatMap(response -> monoIntegerTwo().flatMap(response2 -> monoIntegerException()))
                .onErrorMap(throwable -> {
                    var t = throwable;
                    return new RuntimeException();
                }).subscribe();

        var mono1 = monoIntegerOne();
        var mono2 = monoIntegerTwo();
        var monoError = monoIntegerException().onErrorMap(error -> new RuntimeException());

        Mono.zip(mono1, mono2, monoError).subscribe(tuple -> {
            var t = tuple;
        });
    }


    @Test
    void webfluxTestSubscribe() {
        //Mono Flux
        var mono1 = monoIntegerOne();
        var mono2 = monoIntegerTwo();
        mono1.map(response1 -> {
            System.out.println(response1);
            return 111111;
        }).subscribe(finalReponse -> {
            System.out.println(finalReponse);
        });
        mono2.flatMap(response2 -> {
            System.out.println(response2);
            return monoIntegerOne();
        }).subscribe(response -> {
            System.out.println(response);
        });
    }

    @Test
    void webfluxTestMonoZipSubscribeSecuencial() {
        var mono1 = monoIntegerOne();
        var mono2 = monoIntegerTwo();

        var mono = Mono.zip(mono1, mono2).doOnNext(tuple -> {
            System.out.println("monoIntegerOne completed");
            System.out.println("monoIntegerTwo completed");
        }).then();
        StepVerifier.create(mono).verifyComplete();
    }

    @Test
    void webfluxTestMonoZipSubscribeParallel() {
        var mono1 = monoIntegerOne().subscribeOn(Schedulers.parallel());
        var mono2 = monoIntegerTwo().subscribeOn(Schedulers.parallel());

        var mono = Mono.zip(mono1, mono2).doOnNext(tuple -> {
            System.out.println(tuple);
        }).then();
        StepVerifier.create(mono).verifyComplete();
    }

    @Test
    void webfluxTestDoOnNextCallingAnotherMono() {
        var mono1 = monoIntegerOne();
        var mono2 = monoIntegerTwo();
        var mono3 = monoIntegerThree();

        var mono = Mono.zip(mono1, mono2).map(tuple -> {
            return tuple.getT1();
        }).doOnNext(tuple -> {
            mono3.map(response -> {
                System.out.println(response);
                return 99999999;
            }).subscribe();
        });

        StepVerifier.create(mono).expectNext(1).verifyComplete();
    }

    @Test
    void webfluxTestOperationWithRetry() {

        var mono = monoIntegerOne().flatMap(response -> {
            return monoIntegerException();
        }).retryWhen(Retry.fixedDelay(3, Duration.ofSeconds(2)).filter(element -> element.getClass().equals(RuntimeException.class)));
        StepVerifier.create(mono).verifyError();

    }

    @Test
    void webfluxTestMonoZipWithRetry() {
        var mono1 = monoIntegerOne();
        var mono2 = monoIntegerTwo();

        var publisher = Mono.zip(mono1, mono2).flatMap(tuple -> {
            return monoIntegerException();
        });


        StepVerifier.create(publisher).verifyError();
        //.retryWhen(Retry.fixedDelay(3, Duration.ofSeconds(2)).filter(element->element.getClass().equals(RuntimeException.class))).subscribe();
    }

    @Test
    void webfluxSubscribeBehaviour() {
        var list = List.of(1, 2, 3, 4, 5, 6);
        var publisher = Flux.fromIterable(list)
                .flatMap(this::monoInteger);

        publisher.subscribe();
    }

    @Test
    void webfluxSubscribeForeachBehaviour() {
        var list = List.of(1, 2, 3, 4, 5, 6);
        list.forEach(element -> monoInteger(element).subscribe());
    }

}
