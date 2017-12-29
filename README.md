# gmqtt
MQTT for Golang Client

import (

    "github.com/sanxia/gmqtt"

)

const TOPIC = "sanxia/classroom"

func main() {

    opts := gmqtt.NewClientOptions().AddBroker("tcp://127.0.0.1:1883")

    opts.SetClientID("mqtt-sanxia-2")

    opts.SetKeepAlive(2 * time.Second)

    opts.SetPingTimeout(1 * time.Second)

    opts.SetAutoReconnect(true)

    opts.SetDefaultPublishHandler(func(client gmqtt.Client, msg gmqtt.Message) {
        message := string(msg.Payload()[:])
        fmt.Printf("gmqtt topic: %s, id: %d, msg: %s\n", msg.Topic(), msg.MessageId(), message)
    })

    //连接成功回调
    opts.SetOnConnectHandler(func(client gmqtt.Client) {

        fmt.Println("gmqtt connect OK")

        //订阅

        fmt.Println("gmqtt Subscribe")

        if token := client.Subscribe(TOPIC, 0, nil); token.Wait() && token.Error() != nil {

            fmt.Println(token.Error())

            os.Exit(1)

        }

    })

    //连接失败回调

    opts.SetConnectionLostHandler(func(client gmqtt.Client, err error) {

        fmt.Printf("gmqtt ConnectionLost %v\n", err)

    })

    client := gmqtt.NewClient(opts)

    if token := client.Connect(); token.Wait() && token.Error() != nil {

        panic(token.Error())

    }

    //发布消息
    go func() {
        i := 0

        for true {

            var msg string

            if i%2 == 0 {

                msg = "one"

            } else {

                msg = "two"

            }

            fmt.Println("gmqtt Publish")

            token := client.Publish(TOPIC, 1, true, msg).(*gmqtt.PublishToken)

            fmt.Printf("gmqtt Publish msgId: %d\r\n", token.MessageId())

            token.Wait()

            time.Sleep(2 * time.Second)

            i++

        }

    }()

    time.Sleep(60 * time.Second)

    //取消订阅
    fmt.Println("gmqtt.Unsubscribe")

    if token := client.Unsubscribe(TOPIC); token.Wait() && token.Error() != nil {

        fmt.Println(token.Error())

        os.Exit(1)

    }

    time.Sleep(10 * time.Second)

    fmt.Println("gmqtt Disconnect")

    client.Disconnect(250)

    time.Sleep(3 * time.Second)
}
