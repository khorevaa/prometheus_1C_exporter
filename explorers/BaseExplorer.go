package explorer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	logrusRotate "github.com/LazarenkoA/LogrusRotate"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"
	"io/ioutil"
	"net/http"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	// Канал для передачи флага принудительного обновления данных из МС
	CForce chan bool
)

//////////////////////// Интерфейсы ////////////////////////////
type Isettings interface {
	GetLogPass(string) (log string, pass string)
	RAC_Path() string
	Cluster() string
	GetExplorers() map[string]map[string]interface{}
	GetProperty(string, string, interface{}) interface{}
}

type IExplorers interface {
	StartExplore()
}

type Iexplorer interface {
	Start(IExplorers)
	Stop()
	Pause()
	Continue()
	StartExplore()
	GetName() string
}

//////////////////////// Типы ////////////////////////////

// базовый класс для всех метрик
type BaseExplorer struct {
	sync.Mutex

	mx          *sync.RWMutex
	summary     *prometheus.SummaryVec
	сounter     *prometheus.CounterVec
	gauge       *prometheus.GaugeVec
	ticker      *time.Ticker
	timerNotyfy time.Duration
	settings    Isettings
	cerror      chan error
	ctx         context.Context
	ctxFunc     context.CancelFunc
	//mutex       *sync.Mutex
	isLocked int32
	// mock object
	dataGetter func() ([]map[string]string, error)
}

// базовый класс для всех метрик собираемых через RAC
type BaseRACExplorer struct {
	BaseExplorer

	clusterID string
	one       sync.Once
}

type Metrics struct {
	Explorers []Iexplorer
	Metrics   []string
}

//////////////////////// Методы ////////////////////////////

//func (this *BaseExplorer) Lock(descendant Iexplorer) { // тип middleware
//	//if this.mutex == nil {
//	//	return
//	//}
//
//	logrusRotate.StandardLogger().WithField("Name", descendant.GetName()).Trace("Lock")
//	this.mutex.Lock()
//}

//func (this *BaseExplorer) Unlock(descendant Iexplorer)  {
//	//if this.mutex == nil {
//	//	return
//	//}
//
//	logrusRotate.StandardLogger().WithField("Name", descendant.GetName()).Trace("Unlock")
//	this.mutex.Unlock()
//}

func (this *BaseExplorer) StartExplore() {

}
func (this *BaseExplorer) GetName() string {
	return "Base"
}

func (this *BaseExplorer) run(cmd *exec.Cmd) (string, error) {
	logrusRotate.StandardLogger().WithField("Исполняемый файл", cmd.Path).
		WithField("Параметры", cmd.Args).
		Debug("Выполнение команды")

	timeout := time.Second * 15
	cmd.Stdout = new(bytes.Buffer)
	cmd.Stderr = new(bytes.Buffer)
	errch := make(chan error, 1)

	err := cmd.Start()
	if err != nil {
		return "", fmt.Errorf("Произошла ошибка запуска:\n\terr:%v\n\tПараметры: %v\n\t", err.Error(), cmd.Args)
	}

	// запускаем в горутине т.к. наблюдалось что при выполнении RAC может происходить зависон, нам нужен таймаут
	go func() {
		errch <- cmd.Wait()
	}()

	select {
	case <-time.After(timeout): // timeout
		return "", fmt.Errorf("Выполнение команды прервано по таймауту\n\tПараметры: %v\n\t", cmd.Args)
	case err := <-errch:
		if err != nil {
			stderr := cmd.Stderr.(*bytes.Buffer).String()
			errText := fmt.Sprintf("Произошла ошибка запуска:\n\terr:%v\n\tПараметры: %v\n\t", err.Error(), cmd.Args)
			if stderr != "" {
				errText += fmt.Sprintf("StdErr:%v\n", stderr)
			}

			return "", errors.New(errText)
		} else {

			in := cmd.Stdout.(*bytes.Buffer).Bytes()
			dec := charmap.CodePage866.NewDecoder()

			out, err := dec.Bytes(in)

			//decoded, err :=  decodeBytes()

			if err != nil {
				return "", err
			}

			return string(out), nil
		}
	}
}

func decodeBytes(raw []byte) ([]byte, error) {

	cs := detectFileCharset(raw)

	var Endianness unicode.Endianness

	switch {
	case cs == other:

		// Make a Reader that uses utf16bom:
		unicodeReader := transform.NewReader(bytes.NewReader(raw), charmap.Windows1251.NewDecoder())

		// decode and print:
		decoded, err := ioutil.ReadAll(unicodeReader)
		return decoded, err

	case cs == utf8withBOM:
		return raw[3:], nil
	case cs == utf16Be:
		Endianness = unicode.BigEndian
	case cs == utf16Le:
		Endianness = unicode.LittleEndian
	}

	// Make an tranformer that converts MS-Win default to UTF8:
	win16be := unicode.UTF16(Endianness, unicode.IgnoreBOM)
	// Make a transformer that is like win16be, but abides by BOM:
	utf16bom := unicode.BOMOverride(win16be.NewDecoder())

	// Make a Reader that uses utf16bom:
	unicodeReader := transform.NewReader(bytes.NewReader(raw), utf16bom)

	// decode and print:
	decoded, err := ioutil.ReadAll(unicodeReader)
	return decoded, err

}

type charset byte

const (
	utf8withBOM = charset(iota)
	utf16Be
	utf16Le
	other
)

func detectFileCharset(data []byte) charset {

	// Проверка на BOM
	if len(data) >= 3 {
		switch {
		case data[0] == 0xFF && data[1] == 0xFE:
			return utf16Be
		case data[0] == 0xFE && data[1] == 0xFF:
			return utf16Le
		case data[0] == 0xEF && data[1] == 0xBB && data[2] == 0xBF:
			// wanna Check special ascii codings here?
			return utf8withBOM
		}
	}

	return other
}

// Своеобразный middleware
func (this *BaseExplorer) Start(exp IExplorers) {
	this.ctx, this.ctxFunc = context.WithCancel(context.Background())
	//this.mutex = &sync.Mutex{}

	go func() {
		<-this.ctx.Done() // Stop
		logrusRotate.StandardLogger().Debug("Остановка сбора метрик")

		this.Continue() // что б снять лок
		if this.ticker != nil {
			this.ticker.Stop()
		}
		if this.summary != nil {
			this.summary.Reset()
		}
		if this.gauge != nil {
			this.gauge.Reset()
		}
	}()

	exp.StartExplore()
}

func (this *BaseExplorer) Stop() {
	if this.ctxFunc != nil {
		this.ctxFunc()
	}
}

func (this *BaseExplorer) Pause() {
	logrusRotate.StandardLogger().Trace("Pause. begin")
	defer logrusRotate.StandardLogger().Trace("Pause. end")

	if this.summary != nil {
		this.summary.Reset()
	}
	if this.gauge != nil {
		this.gauge.Reset()
	}

	if atomic.CompareAndSwapInt32(&this.isLocked, 0, 1) { // нужно что бы 2 раза не наложить lock
		this.Lock()
		logrusRotate.StandardLogger().Trace("Pause. Блокировка установлена")
	} else {
		logrusRotate.StandardLogger().WithField("isLocked", this.isLocked).Trace("Pause. Уже заблокировано")
	}
}

func (this *BaseExplorer) Continue() {
	logrusRotate.StandardLogger().Trace("Continue. begin")
	defer logrusRotate.StandardLogger().Trace("Continue. end")

	if atomic.CompareAndSwapInt32(&this.isLocked, 1, 0) {
		this.Unlock()
		logrusRotate.StandardLogger().Trace("Continue. Блокировка снята")
	} else {
		logrusRotate.StandardLogger().WithField("isLocked", this.isLocked).Trace("Continue. Блокировка не была установлена")
	}
}

func (this *BaseRACExplorer) formatMultiResult(data string, licData *[]map[string]string) {
	logrusRotate.StandardLogger().Trace("Парс многострочного результата")

	*licData = []map[string]string{} // очистка
	//reg := regexp.MustCompile(`(?m)^$`)
	values := strings.Split(data, "\r\n")
	logrusRotate.StandardLogger().WithField("count", len(values)).Trace("Парс многострочного результата")

	part := ""

	for _, str := range values {

		part += str + "\n"

		if len(str) == 0 {
			partData := this.formatResult(part)
			if len(partData) == 0 {
				continue
			}
			*licData = append(*licData, partData)
			part = ""
		}

	}
}

func (this *BaseRACExplorer) formatResult(strIn string) map[string]string {
	logrusRotate.StandardLogger().Trace("Парс результата")

	result := make(map[string]string)

	for _, line := range strings.Split(strIn, "\n") {
		parts := strings.Split(line, ":")
		if len(parts) == 2 {
			result[strings.Trim(parts[0], " ")] = strings.Trim(parts[1], " ")
		}
	}

	return result
}

func (this *BaseRACExplorer) mutex() *sync.RWMutex {
	this.one.Do(func() {
		this.mx = new(sync.RWMutex)
	})
	return this.mx
}

func (this *BaseRACExplorer) GetClusterID() string {
	logrusRotate.StandardLogger().Debug("Получаем идентификатор кластера")
	defer logrusRotate.StandardLogger().Debug("Получен идентификатор кластера ", this.clusterID)
	//this.mutex().RLock()
	//defer this.mutex().RUnlock()

	update := func() {
		this.mutex().Lock()
		defer this.mutex().Unlock()

		param := []string{"cluster", "list"}
		if len(this.settings.Cluster()) > 0 {

			param = append(param, this.settings.Cluster())

		}
		cmdCommand := exec.Command(this.settings.RAC_Path(), param...)

		cluster := make(map[string]string)
		if result, err := this.run(cmdCommand); err != nil {
			this.cerror <- fmt.Errorf("Произошла ошибка выполнения при попытки получить идентификатор кластера: \n\t%v", err.Error()) // Если идентификатор кластера не получен нет смысла проболжать работу пиложения
		} else {
			cluster = this.formatResult(result)
		}

		if id, ok := cluster["cluster"]; !ok {
			this.cerror <- errors.New("Не удалось получить идентификатор кластера")
		} else {
			this.clusterID = id
		}
	}

	if this.clusterID == "" {
		// обновляем
		update()
	}

	return this.clusterID
}

func (this *Metrics) Append(ex ...Iexplorer) {
	this.Explorers = append(this.Explorers, ex...)
}

func (this *Metrics) Construct(set Isettings) *Metrics {
	this.Metrics = []string{}
	for k, _ := range set.GetExplorers() {
		this.Metrics = append(this.Metrics, k)
	}

	return this
}

func (this *Metrics) Contains(name string) bool {
	if len(this.Metrics) == 0 {
		return true // Если не задали метрики через парамет, то используем все метрики
	}
	for _, item := range this.Metrics {
		if strings.Trim(item, " ") == strings.Trim(name, " ") {
			return true
		}
	}

	return false
}

func (this *Metrics) findExplorer(name string) Iexplorer {
	for _, item := range this.Explorers {
		if strings.ToLower(item.GetName()) == strings.ToLower(strings.Trim(name, " ")) {
			return item
		}
	}

	return nil
}

func Pause(metrics *Metrics) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, fmt.Sprintf("Метод %q не поддерживается", r.Method), http.StatusInternalServerError)
			return
		}
		logrusRotate.StandardLogger().WithField("URL", r.URL.RequestURI()).Trace("Пауза")

		metricNames := r.URL.Query().Get("metricNames")
		offsetMinStr := r.URL.Query().Get("offsetMin")

		var offsetMin int
		if offsetMinStr != "" {
			if v, err := strconv.ParseInt(offsetMinStr, 0, 0); err == nil {
				offsetMin = int(v)
				logrusRotate.StandardLogger().Infof("Сбор метрик включится автоматически через %d минут", offsetMin)
			} else {
				logrusRotate.StandardLogger().WithError(err).WithField("offsetMin", offsetMinStr).Error("Ошибка конвертации offsetMin")
			}
		}

		logrusRotate.StandardLogger().Infof("Приостановить сбор метрик %q", metricNames)
		for _, metricName := range strings.Split(metricNames, ",") {
			if exp := metrics.findExplorer(metricName); exp != nil {
				exp.Pause()

				// автовключение паузы
				if offsetMin > 0 {
					t := time.NewTicker(time.Minute * time.Duration(offsetMin))
					go func() {
						<-t.C
						exp.Continue()
					}()
				}
			} else {
				fmt.Fprintf(w, "Метрика %q не найдена\n", metricName)
			}
		}
	})
}

func Continue(metrics *Metrics) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, fmt.Sprintf("Метод %q не поддерживается", r.Method), http.StatusInternalServerError)
			return
		}
		logrusRotate.StandardLogger().WithField("URL", r.URL.RequestURI()).Trace("Продолжить")

		metricNames := r.URL.Query().Get("metricNames")
		logrusRotate.StandardLogger().Info("Продолжить сбор метрик", metricNames)
		for _, metricName := range strings.Split(metricNames, ",") {
			if exp := metrics.findExplorer(metricName); exp != nil {
				exp.Continue()
			} else {
				fmt.Fprintf(w, "Метрика %q не найдена", metricName)
				logrusRotate.StandardLogger().Errorf("Метрика %q не найдена", metricName)
			}
		}
	})
}
