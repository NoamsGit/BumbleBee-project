import { BrowserModule } from '@angular/platform-browser';
import { NgModule } from '@angular/core';

import { AppComponent } from './app.component';
import { InfoPanelComponent } from './info-panel/info-panel.component';
import { WeatherPanelComponent } from './weather-panel/weather-panel.component';
import { MapPanelComponent } from './map-panel/map-panel.component';
import { TablePanelComponent } from './table-panel/table-panel.component';
import { HomePageComponent } from './home-page/home-page.component';

@NgModule({
  declarations: [
    AppComponent,
    InfoPanelComponent,
    WeatherPanelComponent,
    MapPanelComponent,
    TablePanelComponent,
    HomePageComponent
  ],
  imports: [
    BrowserModule
  ],
  providers: [],
  bootstrap: [AppComponent]
})
export class AppModule { }
